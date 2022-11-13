package org.leopard.client.internal;

import com.google.protobuf.Message;
import com.google.protobuf.MessageLite;
import com.google.protobuf.Parser;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelPromise;
import io.netty.util.concurrent.Promise;
import org.leopard.client.ClientConfig;
import org.leopard.remote.Type;
import org.leopard.remote.invoke.Callback;
import org.leopard.remote.invoke.GenericInvokeAnswer;
import org.leopard.common.logging.InternalLogger;
import org.leopard.common.logging.InternalLoggerFactory;
import org.leopard.remote.processor.ProcessCommand;
import org.leopard.remote.processor.AwareInvocation;

import java.lang.reflect.Method;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.leopard.remote.util.ByteBufUtil.*;
import static org.leopard.remote.util.ProtoBufUtil.*;

public class OperationInvoker implements ProcessCommand.Server {

    private static final InternalLogger logger = InternalLoggerFactory.getLogger(OperationInvoker.class);

    private final ClientChannel clientChannel;
    private final Semaphore semaphore;

    public OperationInvoker(ClientChannel channel, ClientConfig config) {
        this.clientChannel = channel;
        this.semaphore = new Semaphore(config.getChannelInvokerSemaphore());
    }

    public void invokeMessage(short version, int timeoutMs, Promise<?> promise, MessageLite request, MessageLite extras, ByteBuf message, Class<?> clz) {
        try {
            @SuppressWarnings("unchecked")
            Callback<ByteBuf> callback = assembleInvokeCallback(promise, assembleParser(clz));
            ByteBuf buf = assembleInvokeMessageData(clientChannel.allocator(), request, extras, message);
            invoke0(SEND_MESSAGE, version, buf, (byte) Type.PUSH.sequence(), timeoutMs, callback);
        } catch (Exception e) {
            if (logger.isDebugEnabled()) {
                logger.error(e.getMessage(), e);
            }
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

            if (null != message && message.isReadable()) {
                buf.writeBytes(message, message.readerIndex(), message.readableBytes());
            }

            return buf;
        } catch (Throwable t) {
            release(message);
            throw new RuntimeException(String.format("Failed to assemble invoke message<%s>", request.toString()));
        }
    }

    public void invoke(byte command, int timeoutMs, Promise<?> promise, MessageLite request, Class<?> clz) {
        invoke(command, timeoutMs, (byte) Type.PUSH.sequence(), promise, request, clz);
    }

    public void invoke(byte command, int timeoutMs, byte type, Promise<?> promise, MessageLite request, Class<?> clz) {
        try {
            @SuppressWarnings("unchecked")
            Callback<ByteBuf> callback = assembleInvokeCallback(promise, assembleParser(clz));
            ByteBuf buf = assembleInvokeData(clientChannel.allocator(), request);
            invoke0(command, (short) -1, buf, type, timeoutMs, callback);
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

    private void invoke0(byte command, short version, ByteBuf content, byte type, long timeoutMs, Callback<ByteBuf> callback) {
        try {
            long now = System.currentTimeMillis();
            final Channel channel = clientChannel.channel();
            if (semaphore.tryAcquire(timeoutMs, TimeUnit.MILLISECONDS)) {
                try {
                    if (null == callback) {
                        ChannelPromise promise = channel.newPromise().addListener(f -> semaphore.release());
                        channel.writeAndFlush(AwareInvocation.newInvocation(command, version, retainBuf(content), type), promise);
                    } else {
                        long expired = timeoutMs + now;
                        GenericInvokeAnswer<ByteBuf> answer = new GenericInvokeAnswer<>((buf, cause) -> {
                            semaphore.release();
                            callback.operationCompleted(buf, cause);
                        });
                        channel.writeAndFlush(AwareInvocation.newInvocation(command, version,  retainBuf(content), type, expired, answer));
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
            if (null != callback) {
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
            final String type = null == lite ? null : lite.getClass().getSimpleName();
            throw new RuntimeException("Failed to assemble messageLite type:{" + type + "}", cause);
        }
    }

    private <T> Callback<ByteBuf> assembleInvokeCallback(Promise<T> promise, Parser<T> parser) {
        return null == promise ? null : (buf, cause) -> {
            if (null == cause) {
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
        if (null != promise) {
            promise.tryFailure(t);
        }
    }
}
