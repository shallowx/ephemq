package org.ephemq.remote.client;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.*;
import io.netty.channel.socket.SocketChannel;
import io.netty.util.concurrent.EventExecutorGroup;
import org.ephemq.common.logging.InternalLogger;
import org.ephemq.common.logging.InternalLoggerFactory;
import org.ephemq.remote.codec.MessageDecoder;
import org.ephemq.remote.codec.MessageEncoder;
import org.ephemq.remote.handle.HeartbeatDuplexHandler;
import org.ephemq.remote.handle.ProcessDuplexHandler;
import org.ephemq.remote.invoke.WrappedInvocation;
import org.ephemq.remote.util.NetworkUtil;

import java.nio.charset.StandardCharsets;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * The DemoClientBootstrap class is responsible for bootstrapping a demo client.
 * It sets up and configures the Netty Bootstrap, initializes the channel pipeline,
 * and manages client connections to a predefined server.
 * <p>
 * The class also includes methods to invoke client tasks.
 * The primary entry point to this class is the `main` method.
 */
@SuppressWarnings("all")
public class DemoClientBootstrap {
    public static final InternalLogger logger = InternalLoggerFactory.getLogger(DemoClientBootstrap.class);
    /**
     * A shared instance of Random used for generating random numbers throughout the DemoClientBootstrap class.
     */
    private static final Random RANDOM = new Random();

    /**
     * The entry point of the DemoClientBootstrap application.
     *
     * @param args Command line arguments.
     */
    public static void main(String[] args) {
        EventLoopGroup group = NetworkUtil.newEventLoopGroup(true, 0, "demo-client", false, false);
        EventExecutorGroup serviceGroup = NetworkUtil.newEventExecutorGroup(0, "demo-client-service");
        DemoClientProcessor processor = new DemoClientProcessor();

        try {
            Bootstrap bootstrap = new Bootstrap()
                    .group(group)
                    .channel(NetworkUtil.preferIoUringChannelClass(true, false))
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
                            .addLast("paket-decoder", new MessageDecoder(0))
                            .addLast("connect-handler", new HeartbeatDuplexHandler(20000, 30000))
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
                        if (logger.isErrorEnabled()) {
                            logger.error(e.getMessage(), e);
                        }
                    }
                }, "test-" + i);
                threads[i].start();
            }
            new CountDownLatch(1).await();
        } catch (Exception e) {
            if (logger.isErrorEnabled()) {
                logger.error(e);
            }
        } finally {
            group.shutdownGracefully();
            serviceGroup.shutdownGracefully();
        }
    }

    /**
     * Invokes multiple threads to handle data transfer through a specified channel.
     *
     * @param channel the netty channel through which data is transmitted.
     * @param count the total number of invocations to be performed.
     * @param length the length of the data content in bytes.
     * @param nThread the number of threads to be utilized for invocation.
     * @param size the size of the semaphore that controls concurrent invocations.
     * @param timeout the timeout in milliseconds for each invocation.
     * @param invoker the functional interface implementation that executes the invocation logic.
     * @throws Exception if any exception occurs during invocation.
     */
    private static void invoke(Channel channel, int count, int length, int nThread, int size, int timeout, Invoker invoker) throws Exception {
        byte[] metadata = "This is test metadata".getBytes(StandardCharsets.UTF_8);
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
            }, "invoker-" + i);
            threads[i].start();
        }
        latch.wait();
    }

    /**
     * Invokes a one-way echo operation on the given channel with a specified timeout.
     *
     * The method attempts to acquire a permit from the semaphore within the given timeout.
     * If acquired, it constructs a {@link WrappedInvocation} with the specified data and sends it over the channel.
     * A {@link ChannelPromise} is used to handle the outcome of the operation, releasing the semaphore
     * and logging success or failure information.
     *
     * @param semaphore the {@link Semaphore} to control concurrent access
     * @param channel the {@link Channel} to send the invocation through
     * @param timeout the timeout value in seconds for acquiring the semaphore
     * @param data the {@link ByteBuf} containing the data to be sent with the invocation
     * @throws Exception if an error occurs during the operation or if the timeout expires
     */
    private static void invokeEchoOneway(Semaphore semaphore, Channel channel, int timeout, ByteBuf data) throws Exception {
        long now = System.currentTimeMillis();
        if (semaphore.tryAcquire(timeout, TimeUnit.SECONDS)) {
            long expires = now + timeout + 1000L;
            WrappedInvocation awareInvocation =
                    WrappedInvocation.newInvocation(1, data.retainedSlice(), expires, null);

            ChannelPromise promise = channel.newPromise();
            CountDownLatch countDownLatch = new CountDownLatch(1);

            channel.eventLoop().execute(countDownLatch::countDown);
            countDownLatch.await();

            promise.addListener(f -> {
                semaphore.release();
                long cost = System.currentTimeMillis() - now;
                Throwable cause = f.cause();
                if (cause != null) {
                    logger.error("Failure cost:{}", cost, cause);
                } else {
                    logger.info("Success cost:{}", cost);
                }
            });
            channel.writeAndFlush(awareInvocation, promise);
        } else {
            throw new TimeoutException(String.format("Client invoke echo oneway timeout, local_address:%s, remote_address: %s", channel.localAddress(), channel.remoteAddress()));
        }
    }

    @FunctionalInterface
    interface Invoker {
        /**
         * Invokes an operation on a given channel with a specified timeout and data buffer,
         * controlling concurrent invocations using a semaphore.
         *
         * @param semaphore The Semaphore to control concurrent access.
         * @param channel The Channel through which the invocation is executed.
         * @param timeout The timeout in milliseconds for each invocation.
         * @param data The ByteBuf containing the data to be sent with the invocation.
         * @throws Exception If any exception occurs during the invocation.
         */
        void invoke(Semaphore semaphore, Channel channel, int timeout, ByteBuf data) throws Exception;
    }
}
