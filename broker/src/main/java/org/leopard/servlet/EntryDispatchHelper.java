package org.leopard.servlet;

import io.netty.channel.Channel;
import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.EventExecutorGroup;
import org.leopard.internal.config.BrokerConfig;
import org.leopard.common.logging.InternalLogger;
import org.leopard.common.logging.InternalLoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ThreadLocalRandom;

import static org.leopard.remote.util.NetworkUtils.newEventExecutorGroup;

public class EntryDispatchHelper {

    private static final InternalLogger logger = InternalLoggerFactory.getLogger(EntryDispatchHelper.class);

    private final EventExecutor[] executors;
    private final ConcurrentMap<Channel, EntryEventExecutorHandler> channelOfHandlers = new ConcurrentHashMap<>();
    private final WeakHashMap<EntryEventExecutorHandler, Integer> applyHandlers = new WeakHashMap<>();

    public EntryDispatchHelper(BrokerConfig config) {
        List<EventExecutor> eventExecutorList = new ArrayList<>();

        EventExecutorGroup group = newEventExecutorGroup(config.getMessagePushHandleThreadLimit(), "push-handler-group");
        group.forEach(eventExecutorList::add);
        Collections.shuffle(eventExecutorList);

        executors = eventExecutorList.toArray(new EventExecutor[0]);
    }

    public EventExecutor channelExecutor(Channel channel) {
        return executors[(channel.hashCode() & 0x7fffffff) % executors.length];
    }

    public void putHandler(Channel channel, EntryEventExecutorHandler handler) {
        channelOfHandlers.put(channel, handler);
    }

    public void remove(Channel channel) {
        channelOfHandlers.remove(channel);
    }

    public EntryEventExecutorHandler getHandler(Channel channel) {
        return channelOfHandlers.get(channel);
    }

    public EntryEventExecutorHandler applyHandler(Channel channel, int subscribeLimit) {
        EntryEventExecutorHandler handler = channelOfHandlers.get(channel);
        if (handler != null) {
            return handler;
        }

        synchronized (applyHandlers) {
            if (applyHandlers.isEmpty()) {
                return newHandler();
            }

            ThreadLocalRandom random = ThreadLocalRandom.current();
            int middleLimit = subscribeLimit / 2;
            Map<EntryEventExecutorHandler, Integer> handlers = new HashMap<>();
            int bound = 0;
            for (EntryEventExecutorHandler entryHandler : applyHandlers.keySet()) {
                int channelCount = entryHandler.getChannelShips().size();
                if (channelCount >= subscribeLimit) {
                    continue;
                }

                if (channelCount >= middleLimit) {
                    bound += subscribeLimit - channelCount;
                    handlers.put(entryHandler, channelCount);
                } else if (handler == null || handler.getChannelShips().size() < channelCount) {
                    handler = entryHandler;
                }
            }

            if (handlers.isEmpty() || bound == 0) {
                return handler != null ? handler : newHandler();
            }

            int index = random.nextInt(bound);
            int count = 0;

            for (Map.Entry<EntryEventExecutorHandler, Integer> entry : handlers.entrySet()) {
                count += subscribeLimit - entry.getValue();
                if (index < count) {
                    return entry.getKey();
                }
            }

            return handler != null ? handler : newHandler();
        }
    }

    private EntryEventExecutorHandler newHandler() {
        synchronized (applyHandlers) {
            int[] limitArray = new int[executors.length];
            applyHandlers.values().forEach(i -> limitArray[i]++);

            int index = 0;
            if (limitArray[index] > 0) {
                for (int i = 0; i < limitArray.length; i++) {
                    int limit = limitArray[i];
                    if (limit == 0) {
                        index = i;
                        break;
                    }

                    if (limit < limitArray[index]) {
                        index = i;
                    }
                }
            }

            EntryEventExecutorHandler handler = new EntryEventExecutorHandler(executors[index]);
            applyHandlers.put(handler, index);
            return handler;
        }
    }

    public void close(CloseFunction<Channel, String, String> function) {
        if (applyHandlers.isEmpty()) {
            if(logger.isDebugEnabled()) {
                logger.debug("Available handlers are empty");
            }
            return;
        }

        Set<Map.Entry<Channel, EntryEventExecutorHandler>> entries = channelOfHandlers.entrySet();
        for (Map.Entry<Channel, EntryEventExecutorHandler> entry : entries) {
            Channel channel = entry.getKey();
            EntryEventExecutorHandler handler = getHandler(channel);
            ConcurrentMap<Channel, EntrySubscription> subscriptionShips = handler.getChannelShips();
            if (subscriptionShips == null || subscriptionShips.isEmpty()) {
                continue;
            }

            EntrySubscription entrySubscription = subscriptionShips.get(channel);
            String topic = entrySubscription.getTopic();
            List<String> queues = entrySubscription.getQueue();
            if (queues == null || queues.isEmpty()) {
                continue;
            }

            if (function == null) {
                continue;
            }

            for (String queue : queues) {
                function.consume(channel, topic, queue);
            }

            EventExecutor dispatchExecutor = handler.getDispatchExecutor();
            if (dispatchExecutor.isShutdown()) {
                continue;
            }

            dispatchExecutor.shutdownGracefully();
        }

        applyHandlers.clear();
    }

    @FunctionalInterface
    public interface CloseFunction<T, E, V> {
        void consume(T t, E e, V v);
    }
}
