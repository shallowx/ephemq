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
import org.shallow.ObjectUtil;
import org.shallow.logging.InternalLogger;
import org.shallow.logging.InternalLoggerFactory;
import org.shallow.processor.ProcessCommand;
import org.shallow.util.ByteUtil;
import org.shallow.util.ProtoBufUtil;
import org.shallow.processor.AwareInvocation;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class OperationInvoker implements ProcessCommand.Server {

    private static final InternalLogger logger = InternalLoggerFactory.getLogger(OperationInvoker.class);

    private final ClientChannel clientChannel;
    private final Semaphore semaphore;

    public OperationInvoker(ClientChannel channel, ClientConfig config) {
        this.clientChannel = channel;
        this.semaphore = new Semaphore(config.getChannelInvokerSemaphore());
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
            final Channel channel = clientChannel.channel();;
            if (semaphore.tryAcquire(timeoutMs, TimeUnit.MICROSECONDS)) {
                if (ObjectUtil.isNull(callback)) {
                    ChannelPromise promise = channel.newPromise().addListener(f -> semaphore.release());
                    channel.writeAndFlush(AwareInvocation.newInvocation(command, ByteUtil.retainBuf(content)), promise);
                } else {
                    long expired = timeoutMs + now;
                    InvokeAnswer<ByteBuf> answer = new GenericInvokeAnswer<>((buf, cause) -> {
                        semaphore.release();
                        callback.operationCompleted(buf, cause);
                    });
                    channel.writeAndFlush(AwareInvocation.newInvocation(command, ByteUtil.retainBuf(content), expired, answer));
                }
            } else {
                throw new TimeoutException("Semaphore acquire timeout: " + timeoutMs + "ms");
            }
        } catch (Throwable t) {
            if (ObjectUtil.isNotNull(callback)) {
                callback.operationCompleted(null, new RuntimeException(String.format("Failed to invoke channel, address=%s command=%s", clientChannel.address(), command)));
            }
        } finally {
            ByteUtil.release(content);
        }
    }

    private ByteBuf assembleInvokeData(ByteBufAllocator alloc, MessageLite lite) {
        try {
            return ProtoBufUtil.proto2Buf(alloc, lite);
        } catch (Throwable cause) {
            final String type = ObjectUtil.isNull(lite) ? null : lite.getClass().getSimpleName();
            throw new RuntimeException("[assembleInvokeData] - failed to assemble messageLite type:{" + type + "}", cause);
        }
    }

    private <T> Callback<ByteBuf> assembleInvokeCallback(Promise<T> promise, Parser<T> parser) {
        return ObjectUtil.isNull(promise) ? null : (buf, cause) -> {
            if (ObjectUtil.isNull(cause)) {
                try {
                    promise.trySuccess(ProtoBufUtil.readProto(buf, parser));
                } catch (Throwable t) {
                    promise.tryFailure(t);
                }
            } else {
                promise.tryFailure(cause);
            }
        };
    }

    private static void tryFailure(Promise<?> promise, Throwable t) {
        if (ObjectUtil.isNotNull(promise)) {
            promise.tryFailure(t);
        }
    }
}
