package org.ostara.remote;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.*;
import io.netty.channel.socket.SocketChannel;
import io.netty.util.concurrent.EventExecutorGroup;
import org.ostara.common.logging.InternalLogger;
import org.ostara.common.logging.InternalLoggerFactory;
import org.ostara.remote.codec.MessageDecoder;
import org.ostara.remote.codec.MessageEncoder;
import org.ostara.remote.handle.ConnectDuplexHandler;
import org.ostara.remote.handle.ProcessDuplexHandler;
import org.ostara.remote.processor.AwareInvocation;
import org.ostara.remote.util.NetworkUtils;

import java.nio.charset.StandardCharsets;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

@SuppressWarnings("all")
public class DemoClientBootstrap {
    public static final InternalLogger logger = InternalLoggerFactory.getLogger(DemoClientBootstrap.class);
    private static final Random RANDOM = new Random();
    public static void main(String[] args) {
        EventLoopGroup group = NetworkUtils.newEventLoopGroup(true, 0, "demo-client");
        EventExecutorGroup serviceGroup = NetworkUtils.newEventExecutorGroup(0, "demo-client-services");
        DemoClientProcessor processor = new DemoClientProcessor();

        try {
            Bootstrap bootstrap = new Bootstrap()
                    .group(group)
                    .channel(NetworkUtils.preferChannelClass(true))
                    .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 3000)
                    .option(ChannelOption.TCP_NODELAY, true)
                    .option(ChannelOption.SO_KEEPALIVE, false)
                    .option(ChannelOption.SO_SNDBUF, 65536)
                    .option(ChannelOption.SO_RCVBUF, 65536);

            bootstrap.handler(new ChannelInitializer<SocketChannel>() {
                @Override
                protected void initChannel(SocketChannel socketChannel) throws Exception {
                    socketChannel.pipeline()
                            .addLast("packet-encoder", MessageEncoder.instance())
                            .addLast("paket-decoder", new MessageDecoder())
                            .addLast("connect-handler", new ConnectDuplexHandler(20000, 30000))
                            .addLast("service-handler", new ProcessDuplexHandler(processor));
                }
            });

            Thread[] threads = new Thread[1];
            for (int i = 0; i < threads.length; i++) {
                threads[i] = new Thread(() -> {
                    try {
                        Channel channel = bootstrap.connect("127.0.0.1", 8888).sync().channel();
                        invoke(channel, Integer.MAX_VALUE, 1024, 1, 5000, 2, DemoClientBootstrap::invokeEchoOneway);
                    } catch (Exception e) {
                        logger.error(e.getMessage(), e);
                    }
                }, "TEST:" + i);
                threads[i].start();
            }
            new CountDownLatch(1).await();
        }catch (Exception e) {
            logger.error(e);
        } finally {
            group.shutdownGracefully();
            serviceGroup.shutdownGracefully();
        }
    }

    private static void invoke(Channel channel, int count, int length, int nThread, int size, int timeout, Invoker invoker) throws Exception {
        byte[] metadata = "Test metadata".getBytes(StandardCharsets.UTF_8);
        byte[] content = new byte[length];
        RANDOM.nextBytes(content);

        ByteBuf data = Unpooled.buffer(4 + metadata.length + content.length);
        data.writeInt(metadata.length);
        data.writeBytes(metadata);
        data.writeBytes(content);

        AtomicInteger countIndex = new AtomicInteger();
        CountDownLatch latch = new CountDownLatch(nThread);
        Thread[] threads = new Thread[nThread];
        for (int i = 0; i < nThread; i++) {
            threads[i] = new Thread(() -> {
                Semaphore semaphore = new Semaphore(size);
                int waitCount = 0;
                long lastTime = System.currentTimeMillis();
                while (countIndex.incrementAndGet() < count) {
                    if (!channel.isActive()) {
                        return;
                    }
                    waitCount++;
                    try {
                        invoker.invoke(semaphore, channel, timeout, data);
                    } catch (Exception e) {
                        logger.error(e);
                    }

                    long now = System.currentTimeMillis();
                    if ((now - lastTime) > 10000) {
                        long interval = now - lastTime;
                        long qps = waitCount / (interval / 1000);
                        double avgRT = waitCount > 0 ? 1D * interval / waitCount : -1;
                        logger.info(String.format("[%s] QPS: %d avgRT:%.3fms", Thread.currentThread().getName(), qps, avgRT));
                        lastTime = now;
                        waitCount = 0;
                    }
                }
                latch.countDown();
            }, "Invoker-" + i);
            threads[i].start();
        }
        latch.wait();
    }

    private static void invokeEchoOneway(Semaphore semaphore, Channel channel, int timeout, ByteBuf data) throws Exception {
        long now = System.currentTimeMillis();
        if (semaphore.tryAcquire(timeout, TimeUnit.SECONDS)) {
            long expires = now + timeout + 1000L;
            AwareInvocation awareInvocation = AwareInvocation.newInvocation(1, data.retainedSlice(), expires, null);

            ChannelPromise promise = channel.newPromise();
            CountDownLatch countDownLatch = new CountDownLatch(1);

            channel.eventLoop().execute(countDownLatch::countDown);
            countDownLatch.await();

            promise.addListener(f -> {
                semaphore.release();
                long cost = System.currentTimeMillis() - now;
                Throwable cause = f.cause();
                if (cause != null) {
                    logger.error("FAILURE cost:{}", cost, cause);
                } else {
                    logger.info("SUCCESS cost:{}", cost);
                }
            });
            channel.writeAndFlush(awareInvocation, promise);
        } else {
            throw new TimeoutException();
        }
    }

    @FunctionalInterface
    interface Invoker{
        void invoke(Semaphore semaphore, Channel channel, int timeout, ByteBuf data) throws Exception;
    }
}
