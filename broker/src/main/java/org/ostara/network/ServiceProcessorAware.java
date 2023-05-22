package org.ostara.network;

import com.google.inject.Inject;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.ImmediateEventExecutor;
import org.ostara.common.logging.InternalLogger;
import org.ostara.common.logging.InternalLoggerFactory;
import org.ostara.core.Config;
import org.ostara.log.Log;
import org.ostara.management.Manager;
import org.ostara.remote.RemoteException;
import org.ostara.remote.invoke.InvokeAnswer;
import org.ostara.remote.processor.ProcessCommand;
import org.ostara.remote.processor.ProcessorAware;

public class ServiceProcessorAware implements ProcessorAware, ProcessCommand.Server {

    private static final InternalLogger logger = InternalLoggerFactory.getLogger(ServiceProcessorAware.class);
    private Config config;
    private Manager manager;
    private EventExecutor commandExecutor;
    private EventExecutor serviceExecutor;

    @Inject
    public ServiceProcessorAware(Config config, Manager manager) {
        this.config = config;
        this.manager = manager;
        this.commandExecutor = manager.getCommandHandleEventExecutorGroup().next();
    }

    @Override
    public void onActive(Channel channel, EventExecutor executor) {
        this.serviceExecutor = executor;
        manager.getConnectionManager().add(channel);
        channel.closeFuture().addListener(future -> {
            for (Log log : manager.getLogManager().getLedgerId2LogMap().values()) {
                log.cleanSubscribe(channel, ImmediateEventExecutor.INSTANCE.newPromise());
            }
        });
    }

    @Override
    public void process(Channel channel, int code, ByteBuf data, InvokeAnswer<ByteBuf> answer) {
        int length = data.readableBytes();
        try {
            switch (code) {
                case SEND_MESSAGE -> processSendMessage(channel, code, data, answer);
                case QUERY_CLUSTER_INFOS -> processQueryClusterInfo(channel, code, data, answer);
                case QUERY_TOPIC_INFOS -> processQueryTopicInfos(channel, code, data, answer);
                case REST_SUBSCRIBE -> processRestSubscription(channel, code, data, answer);
                case ALTER_SUBSCRIBE -> processAlterSubscription(channel, code, data, answer);
                case CLEAN_SUBSCRIBE -> processCleanSubscription(channel, code, data, answer);
                case CREATE_TOPIC -> processCreateTopic(channel, code, data, answer);
                case DELETE_TOPIC -> processDeleteTopic(channel, code, data, answer);
                default -> {
                    if (answer != null) {
                        String error = "Command[" + code + "] unsupported";
                        answer.failure(RemoteException.of(RemoteException.Failure.UNSUPPORTED_EXCEPTION, error));
                    }
                }
            }
        } catch (Throwable t) {
            if (answer != null) {
                answer.failure(t);
            }
        }
    }

    private void processSendMessage(Channel channel, int code, ByteBuf data, InvokeAnswer<ByteBuf> answer) {

    }

    private void processQueryClusterInfo(Channel channel, int code, ByteBuf data, InvokeAnswer<ByteBuf> answer) {

    }
    private void processQueryTopicInfos(Channel channel, int code, ByteBuf data, InvokeAnswer<ByteBuf> answer) {

    }
    private void processRestSubscription(Channel channel, int code, ByteBuf data, InvokeAnswer<ByteBuf> answer) {

    }

    private void processAlterSubscription(Channel channel, int code, ByteBuf data, InvokeAnswer<ByteBuf> answer) {

    }

    private void processCleanSubscription(Channel channel, int code, ByteBuf data, InvokeAnswer<ByteBuf> answer) {

    }

    private void processCreateTopic(Channel channel, int code, ByteBuf data, InvokeAnswer<ByteBuf> answer) {

    }

    private void processDeleteTopic(Channel channel, int code, ByteBuf data, InvokeAnswer<ByteBuf> answer) {

    }
}
