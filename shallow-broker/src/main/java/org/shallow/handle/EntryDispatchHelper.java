package org.shallow.handle;

import io.netty.channel.Channel;
import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.EventExecutorGroup;
import org.shallow.internal.config.BrokerConfig;
import org.shallow.logging.InternalLogger;
import org.shallow.logging.InternalLoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ThreadLocalRandom;

import static org.shallow.util.NetworkUtil.newEventExecutorGroup;

public class EntryDispatchHelper {

    private static final InternalLogger logger = InternalLoggerFactory.getLogger(EntryDispatchHelper.class);

    private final EventExecutor[] executors;
    private final ConcurrentMap<Channel, EntryPushHandler> channelOfHandlers = new ConcurrentHashMap<>();
    private final WeakHashMap<EntryPushHandler, Integer> applyHandlers = new WeakHashMap<>();

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

    public void putHandler(Channel channel, EntryPushHandler handler) {
        channelOfHandlers.put(channel, handler);
    }

    public void remove(Channel channel) {
        channelOfHandlers.remove(channel);
    }

    public EntryPushHandler getHandler(Channel channel) {
        return channelOfHandlers.get(channel);
    }

    public EntryPushHandler applyHandler(Channel channel, int subscribeLimit) {
        EntryPushHandler handler = channelOfHandlers.get(channel);
        if (handler != null) {
            return handler;
        }

        synchronized (applyHandlers) {
            if (applyHandlers.isEmpty()) {
                return newHandler();
            }

            ThreadLocalRandom random = ThreadLocalRandom.current();
            int middleLimit = subscribeLimit / 2;
            Map<EntryPushHandler, Integer> handlers = new HashMap<>();
            int bound = 0;
            for (EntryPushHandler entryHandler : applyHandlers.keySet()) {
                int channelCount = entryHandler.getSubscriptionShips().size();
                if (channelCount >= subscribeLimit) {
                    continue;
                }

                if (channelCount >= middleLimit) {
                    bound += subscribeLimit - channelCount;
                    handlers.put(entryHandler, channelCount);
                } else if (handler == null || handler.getSubscriptionShips().size() < channelCount) {
                    handler = entryHandler;
                }
            }

            if (handlers.isEmpty() || bound == 0) {
                return handler != null ? handler : newHandler();
            }

            int index = random.nextInt(bound);
            int count = 0;

            for (Map.Entry<EntryPushHandler, Integer> entry : handlers.entrySet()) {
                count += subscribeLimit - entry.getValue();
                if (index < count) {
                    return entry.getKey();
                }
            }

            return handler != null ? handler : newHandler();
        }
    }

    private EntryPushHandler newHandler() {
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

            EntryPushHandler handler = new EntryPushHandler(executors[index]);
            applyHandlers.put(handler, index);
            return handler;
        }
    }

    public void close(CloseFunction<Channel, String, String> function) {
        if (applyHandlers.isEmpty()) {
            return;
        }

        Set<Map.Entry<Channel, EntryPushHandler>> entries = channelOfHandlers.entrySet();
        for (Map.Entry<Channel, EntryPushHandler> entry : entries) {
            Channel channel = entry.getKey();
            EntryPushHandler handler = getHandler(channel);
            ConcurrentMap<Channel, EntrySubscription> subscriptionShips = handler.getSubscriptionShips();
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
