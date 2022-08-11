package org.shallow.invoke;

import com.google.protobuf.Message;
import com.google.protobuf.MessageLite;
import com.google.protobuf.Parser;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelPromise;
import io.netty.util.concurrent.ImmediateEventExecutor;
import io.netty.util.concurrent.Promise;
import org.shallow.ClientConfig;
import org.shallow.logging.InternalLogger;
import org.shallow.logging.InternalLoggerFactory;
import org.shallow.processor.ProcessCommand;
import org.shallow.processor.AwareInvocation;
import org.shallow.util.NetworkUtil;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.shallow.util.NetworkUtil.newImmediatePromise;
import static org.shallow.util.ObjectUtil.isNotNull;
import static org.shallow.util.ObjectUtil.isNull;
import static org.shallow.util.ByteUtil.release;
import static org.shallow.util.ByteUtil.retainBuf;
import static org.shallow.util.ProtoBufUtil.proto2Buf;
import static org.shallow.util.ProtoBufUtil.readProto;

public class OperationInvoker implements ProcessCommand.Server {

    private static final InternalLogger logger = InternalLoggerFactory.getLogger(OperationInvoker.class);

    private final ClientChannel clientChannel;
    private final Semaphore semaphore;

    public OperationInvoker(ClientChannel channel, ClientConfig config) {
        this.clientChannel = channel;
        this.semaphore = new Semaphore(config.getChannelInvokerSemaphore());
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
   private Parser assembleParser(Class<?> clz) throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        final Method defaultInstance = clz.getDeclaredMethod("getDefaultInstance");
        Message defaultInst = (Message) defaultInstance.invoke(null);
       return defaultInst.getParserForType();
   }

    private void invoke0(byte command, ByteBuf content, long timeoutMs, Callback<ByteBuf> callback) {
        try {
            long now = System.currentTimeMillis();
            final Channel channel = clientChannel.channel();
            if (semaphore.tryAcquire(timeoutMs, TimeUnit.MILLISECONDS)) {
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
            } else {
                throw new TimeoutException("[Invoke0] - semaphore acquire timeout: " + timeoutMs + "ms");
            }
        } catch (Throwable t) {
            RuntimeException exception = new RuntimeException(String.format("[Invoke0] - failed to invoke channel, address=%s command=%s", clientChannel.address(), command));
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
            throw new RuntimeException("[AssembleInvokeData] - failed to assemble messageLite type:{" + type + "}", cause);
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
