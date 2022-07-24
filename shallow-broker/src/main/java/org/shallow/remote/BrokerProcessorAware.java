package org.shallow.remote;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.util.concurrent.Future;
import org.shallow.RemoteException;
import org.shallow.internal.BrokerManager;
import org.shallow.invoke.InvokeAnswer;
import org.shallow.logging.InternalLogger;
import org.shallow.logging.InternalLoggerFactory;
import org.shallow.processor.ProcessCommand;
import org.shallow.processor.ProcessorAware;
import org.shallow.proto.CreateTopicAnswer;
import org.shallow.proto.CreateTopicRequest;
import org.shallow.topic.TopicMetadata;

import static org.shallow.ObjectUtil.isNotNull;
import static org.shallow.util.NetworkUtil.switchAddress;
import static org.shallow.util.ProtoBufUtil.proto2Buf;
import static org.shallow.util.ProtoBufUtil.readProto;

public class BrokerProcessorAware implements ProcessorAware, ProcessCommand.Server {

    private static final InternalLogger logger = InternalLoggerFactory.getLogger(BrokerSocketServer.class);

    private final BrokerManager manager;

    public BrokerProcessorAware(BrokerManager manager) {
        this.manager = manager;
    }

    @Override
    public void onActive(ChannelHandlerContext ctx) {
        if (logger.isInfoEnabled()) {
            logger.info("Obtain remote active address <{}>", ctx.channel().remoteAddress());
        }
    }

    @Override
    public void process(Channel channel, byte command, ByteBuf data, InvokeAnswer<ByteBuf> answer) {
        try {
            switch (command) {
                case CREATE_TOPIC -> {
                    final CreateTopicRequest request = readProto(data, CreateTopicRequest.parser());
                    final String topic = request.getName();
                    TopicMetadata topicMetadata = new TopicMetadata(topic);

                    @SuppressWarnings("unchecked")
                    Future<Boolean> future = manager.getTopicProvider().append(topicMetadata);
                    logger.info("[process] topic={}", topic);

                    final CreateTopicAnswer response = CreateTopicAnswer.newBuilder().setAck(future.get() ? InvokeAnswer.SUCCESS : InvokeAnswer.FAILURE).build();
                    if (isNotNull(answer)) {
                        answer.success(proto2Buf(channel.alloc(), response));
                    }
                }
                case DELETE_TOPIC -> {}
                case UPDATE_TOPIC -> {}
                case FETCH_CLUSTER_INFO -> {}
                case FETCH_TOPIC_INFO -> {}
                default -> {
                    if (logger.isInfoEnabled()) {
                        logger.info("[Server process] <{}> - not supported command [{}]", switchAddress(channel), command);
                    }
                    answerFailed(answer, RemoteException.of(RemoteException.Failure.UNSUPPORTED_EXCEPTION, "Not supported command ["+ command +"]"));
                }
            }
        } catch (Throwable cause) {
            if (logger.isErrorEnabled()) {
                logger.error("#");
            }
            answerFailed(answer, cause);
        }
    }

    private void answerFailed(InvokeAnswer<ByteBuf> answer, Throwable cause) {
        if (isNotNull(answer)) {
            answer.failure(cause);
        }
    }
}
