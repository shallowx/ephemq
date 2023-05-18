package org.ostara.dispatch;

import static org.ostara.remote.util.NetworkUtils.newEventExecutorGroup;
import io.netty.channel.Channel;
import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.EventExecutorGroup;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.WeakHashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ThreadLocalRandom;
import org.ostara.common.logging.InternalLogger;
import org.ostara.common.logging.InternalLoggerFactory;
import org.ostara.config.ServerConfig;

public class DispatchProcessorSupport {

    private static final InternalLogger logger = InternalLoggerFactory.getLogger(DispatchProcessorSupport.class);

    private final EventExecutor[] executors;
    private final ConcurrentMap<Channel, EntryEventExecutorHandler> channelOfHandlers = new ConcurrentHashMap<>();
    private final WeakHashMap<EntryEventExecutorHandler, Integer> eventExecutorHandlers = new WeakHashMap<>();

    public DispatchProcessorSupport(ServerConfig config, String name) {
        List<EventExecutor> eventExecutorList = new ArrayList<>();

        EventExecutorGroup group = newEventExecutorGroup(config.getMessageHandleThreadLimit(), name);
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

    public EntryEventExecutorHandler accessCheckedHandler(Channel channel, int subscribeLimit) {
        EntryEventExecutorHandler handler = channelOfHandlers.get(channel);
        if (handler != null) {
            return handler;
        }

        synchronized (eventExecutorHandlers) {
            if (eventExecutorHandlers.isEmpty()) {
                return newHandler();
            }

            ThreadLocalRandom random = ThreadLocalRandom.current();
            int middleLimit = subscribeLimit / 2;
            Map<EntryEventExecutorHandler, Integer> handlers = new HashMap<>();
            int bound = 0;
            for (EntryEventExecutorHandler entryHandler : eventExecutorHandlers.keySet()) {
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
        synchronized (eventExecutorHandlers) {
            int[] limitArray = new int[executors.length];
            eventExecutorHandlers.values().forEach(i -> limitArray[i]++);

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
            eventExecutorHandlers.put(handler, index);
            return handler;
        }
    }

    public void close(CloseFunction<Channel, String, String> function) {
        if (eventExecutorHandlers.isEmpty()) {
            logger.debug("Available handlers are empty");
            return;
        }

        Set<Map.Entry<Channel, EntryEventExecutorHandler>> entries = channelOfHandlers.entrySet();
        for (Map.Entry<Channel, EntryEventExecutorHandler> entry : entries) {
            Channel channel = entry.getKey();
            EntryEventExecutorHandler handler = getHandler(channel);
            ConcurrentMap<Channel, Subscription> subscriptionShips = handler.getChannelShips();
            if (subscriptionShips == null || subscriptionShips.isEmpty()) {
                continue;
            }

            Subscription entrySubscription = subscriptionShips.get(channel);
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

        eventExecutorHandlers.clear();
    }

    @FunctionalInterface
    public interface CloseFunction<T, E, V> {
        void consume(T t, E e, V v);
    }
}
