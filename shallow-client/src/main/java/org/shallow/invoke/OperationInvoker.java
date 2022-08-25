package org.shallow.invoke;

import com.google.protobuf.Message;
import com.google.protobuf.MessageLite;
import com.google.protobuf.Parser;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelPromise;
import io.netty.util.concurrent.Promise;
import org.shallow.ClientConfig;
import org.shallow.logging.InternalLogger;
import org.shallow.logging.InternalLoggerFactory;
import org.shallow.processor.ProcessCommand;
import org.shallow.processor.AwareInvocation;
import org.shallow.util.ByteBufUtil;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.shallow.util.ByteBufUtil.*;
import static org.shallow.util.ObjectUtil.isNotNull;
import static org.shallow.util.ObjectUtil.isNull;
import static org.shallow.util.ProtoBufUtil.*;

public class OperationInvoker implements ProcessCommand.Server {

    private static final InternalLogger logger = InternalLoggerFactory.getLogger(OperationInvoker.class);

    private final ClientChannel clientChannel;
    private final Semaphore semaphore;

    public OperationInvoker(ClientChannel channel, ClientConfig config) {
        this.clientChannel = channel;
        this.semaphore = new Semaphore(config.getChannelInvokerSemaphore());
    }

    public void invokeMessage(int timeoutMs, Promise<?> promise, MessageLite request, MessageLite extras, ByteBuf message, Class<?> clz) {
        try {
            @SuppressWarnings("unchecked")
            Callback<ByteBuf> callback = assembleInvokeCallback(promise, assembleParser(clz));
            ByteBuf buf = assembleInvokeMessageData(clientChannel.allocator(), request, extras, message);
            invoke0(SEND_MESSAGE, buf, timeoutMs, callback);
        } catch (Exception e) {
            tryFailure(promise, e);
        }
    }

    private ByteBuf assembleInvokeMessageData(ByteBufAllocator alloc, MessageLite request, MessageLite extras, ByteBuf message) {
        ByteBuf buf;
        try {
            int length = protoLength(request) + protoLength(extras) + bufLength(message);
            buf = alloc.ioBuffer(length);

            writeProto(buf, request);
            writeProto(buf, extras);

            if (isNotNull(message) && message.isReadable()) {
                buf.writeBytes(message, message.readerIndex(), message.readableBytes());
            }

            return buf;
        } catch (Throwable t) {
            ByteBufUtil.release(message);
            throw new RuntimeException("Failed to assemble invoke message");
        }
    }

    public void invoke(byte command, int timeoutMs, MessageLite request, Class<?> clz) {
        invoke(command, timeoutMs, null, request, clz);
    }

    public void invoke(byte command, int timeoutMs, Promise<?> promise, MessageLite request, Class<?> clz) {
       try {
           @SuppressWarnings("unchecked")
           Callback<ByteBuf> callback = assembleInvokeCallback(promise, assembleParser(clz));
           ByteBuf buf = assembleInvokeData(clientChannel.allocator(), request);
           invoke0(command, buf, timeoutMs, callback);
       } catch (Exception e) {
           tryFailure(promise, e);
       }
   }

   @SuppressWarnings("rawtypes")
   private Parser assembleParser(Class<?> clz) throws Exception {
        final Method defaultInstance = clz.getDeclaredMethod("getDefaultInstance");
        Message defaultInst = (Message) defaultInstance.invoke(null);
       return defaultInst.getParserForType();
   }

    private void invoke0(byte command, ByteBuf content, long timeoutMs, Callback<ByteBuf> callback) {
        try {
            long now = System.currentTimeMillis();
            final Channel channel = clientChannel.channel();
            if (semaphore.tryAcquire(timeoutMs, TimeUnit.MILLISECONDS)) {
                try {
                    if (isNull(callback)) {
                        ChannelPromise promise = channel.newPromise().addListener(f -> semaphore.release());
                        channel.writeAndFlush(AwareInvocation.newInvocation(command, retainBuf(content)), promise);
                    } else {
                        long expired = timeoutMs + now;
                        GenericInvokeAnswer<ByteBuf> answer = new GenericInvokeAnswer<>((buf, cause) -> {
                            semaphore.release();
                            callback.operationCompleted(buf, cause);
                        });
                        channel.writeAndFlush(AwareInvocation.newInvocation(command, retainBuf(content), expired, answer));
                    }
                } catch (Throwable t) {
                    semaphore.release();
                    throw t;
                }
            } else {
                throw new TimeoutException("Semaphore acquire timeout: " + timeoutMs + "ms");
            }
        } catch (Throwable t) {
            RuntimeException exception = new RuntimeException(String.format("Failed to invoke channel, address=%s command=%s", clientChannel.address(), command));
            if (isNotNull(callback)) {
                callback.operationCompleted(null, exception);
            } else {
                throw exception;
            }
        } finally {
            release(content);
        }
    }

    private ByteBuf assembleInvokeData(ByteBufAllocator alloc, MessageLite lite) {
        try {
            return proto2Buf(alloc, lite);
        } catch (Throwable cause) {
            final String type = isNull(lite) ? null : lite.getClass().getSimpleName();
            throw new RuntimeException("Failed to assemble messageLite type:{" + type + "}", cause);
        }
    }

    private <T> Callback<ByteBuf> assembleInvokeCallback(Promise<T> promise, Parser<T> parser) {
        return isNull(promise) ? null : (buf, cause) -> {
            if (isNull(cause)) {
                try {
                    promise.trySuccess(readProto(buf, parser));
                } catch (Throwable t) {
                    promise.tryFailure(t);
                }
            } else {
                promise.tryFailure(cause);
            }
        };
    }

    private static void tryFailure(Promise<?> promise, Throwable t) {
        if (isNotNull(promise)) {
            promise.tryFailure(t);
        }
    }
}
