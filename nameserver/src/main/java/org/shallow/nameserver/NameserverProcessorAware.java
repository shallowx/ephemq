package org.shallow.nameserver;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import io.netty.util.concurrent.Promise;
import org.shallow.Manager;
import org.shallow.NameserverConfig;
import org.shallow.common.logging.InternalLogger;
import org.shallow.common.logging.InternalLoggerFactory;
import org.shallow.metadata.TopicManager;
import org.shallow.remote.RemoteException;
import org.shallow.remote.invoke.InvokeAnswer;
import org.shallow.remote.processor.Ack;
import org.shallow.remote.processor.ProcessCommand;
import org.shallow.remote.processor.ProcessorAware;
import org.shallow.remote.proto.server.CreateTopicResponse;
import org.shallow.remote.util.NetworkUtil;

import java.io.IOException;

import static org.shallow.remote.util.NetworkUtil.switchAddress;
import static org.shallow.remote.util.ProtoBufUtil.proto2Buf;

@SuppressWarnings("all")
public class NameserverProcessorAware implements ProcessorAware, ProcessCommand.Nameserver {

    private static final InternalLogger logger = InternalLoggerFactory.getLogger(NameserverProcessorAware.class);

    private final NameserverConfig config;
    private final Manager manager;

    public NameserverProcessorAware(NameserverConfig config, Manager manager) {
        this.config = config;
        this.manager = manager;
    }

    @Override
    public void onActive(Channel channel, EventExecutor executor) {
        if (logger.isDebugEnabled()) {
            logger.debug("Get remote active address<{}> successfully", channel.remoteAddress().toString());
        }
    }

    @Override
    public void process(Channel channel, byte command, ByteBuf data, InvokeAnswer<ByteBuf> answer, byte type, short version) {
        try {
            switch (command) {
                case NEW_NODE -> processNewNodeRequest(channel, data, answer);

                case NEW_TOPIC -> processNewTopicRequest(channel, data, answer);

                case QUERY_NODE -> processQueryNodeRequest(channel, data, answer);

                case QUERY_TOPIC -> processQueryTopicRequest(channel, data, answer);

                case HEARTBEAT -> processHeartbeatRequest(channel, data, answer);

                default -> {
                    if (logger.isDebugEnabled()) {
                        logger.debug("Channel<{}> - not supported command [{}]", switchAddress(channel), command);
                    }
                    answerFailed(answer, RemoteException.of(RemoteException.Failure.UNSUPPORTED_EXCEPTION, "Not supported command ["+ command +"]"));
                }
            }
        } catch (Throwable cause) {
            if (logger.isErrorEnabled()) {
                logger.error("Channel<{}> - command [{}]", switchAddress(channel), command);
            }
            answerFailed(answer, cause);
        }
    }

    private void processNewTopicRequest(Channel channel, ByteBuf data, InvokeAnswer<ByteBuf> answer) {
        try {
            Promise<Void> promise = NetworkUtil.newImmediatePromise();
            promise.addListener(new GenericFutureListener<Future<Void>>() {
                @Override
                public void operationComplete(Future<Void> future) throws Exception {
                    if (future.isSuccess()) {
                        if (answer != null) {
                            CreateTopicResponse response = CreateTopicResponse
                                    .newBuilder()
                                    .setAck(Ack.SUCCESS)
                                    .build();

                            answer.success(proto2Buf(channel.alloc(), response));
                        }
                    } else {
                        answerFailed(answer, future.cause());
                    }
                }
            });

            TopicManager topicManager = manager.getTopicManager();
            topicManager.add();
        } catch (Throwable t) {
            answerFailed(answer, t);
        }
    }

    private void processNewNodeRequest(Channel channel, ByteBuf data, InvokeAnswer<ByteBuf> answer) {

    }

    private void processQueryTopicRequest(Channel channel, ByteBuf data, InvokeAnswer<ByteBuf> answer) {

    }

    private void processQueryNodeRequest(Channel channel, ByteBuf data, InvokeAnswer<ByteBuf> answer) {

    }

    private void processHeartbeatRequest(Channel channel, ByteBuf data, InvokeAnswer<ByteBuf> answer) {

    }

    private void answerFailed(InvokeAnswer<ByteBuf> answer, Throwable cause) {
        if (answer != null) {
            answer.failure(cause);
        }
    }
}
