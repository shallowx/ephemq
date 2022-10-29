package org.shallow.handle;

import io.netty.util.concurrent.EventExecutor;
import org.shallow.internal.atomic.DistributedAtomicInteger;
import org.shallow.internal.config.BrokerConfig;
import org.shallow.common.logging.InternalLogger;
import org.shallow.common.logging.InternalLoggerFactory;

import javax.annotation.concurrent.ThreadSafe;
import java.util.Comparator;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.stream.Collectors;

import static org.shallow.remote.util.NetworkUtil.newEventExecutorGroup;

@ThreadSafe
public class EntryPullHandlerExecutionMapping implements HandlerMapping {

    private static final InternalLogger logger = InternalLoggerFactory.getLogger(EntryPullHandlerExecutionMapping.class);

    private final BrokerConfig config;
    private final Set<EntryPullHandler> handlers = new CopyOnWriteArraySet<>();
    private final DistributedAtomicInteger atomicValue = new DistributedAtomicInteger();

    public EntryPullHandlerExecutionMapping(BrokerConfig config) {
        this.config = config;
    }

    @Override
    public EntryPullHandler applyHandler() {
        if (handlers.isEmpty()) {
            return buildHandler();
        }

        EntryPullHandler minHandler = handlers.stream()
                .sorted(Comparator.comparingInt(EntryPullHandler::count))
                .collect(Collectors.toCollection(LinkedHashSet::new))
                .iterator()
                .next();

        if (minHandler.count() >= minHandler.limit()) {
            return buildHandler();
        }

        minHandler.setCount(minHandler.count() + 1);
        return minHandler;
    }

    @Override
    public EntryPullHandler preHandle(EntryPullHandler handler) {
        EventExecutor executor = handler.executor();
        if (executor.isShutdown()) {
            if (logger.isWarnEnabled()) {
                logger.warn("The request<{}> handler event executor is shutdown", handler.id());
            }
            return applyHandler(handler);
        }

        if (executor.isTerminated()) {
            if (logger.isWarnEnabled()) {
                logger.warn("The request<{}> handler event executor is terminated", handler.id());
            }
            return applyHandler(handler);
        }
        return handler;
    }

    private EntryPullHandler applyHandler(EntryPullHandler handler) {
        if (handlers.isEmpty()) {
            return buildHandler();
        }

        EntryPullHandler minHandler = handlers.stream()
                .filter(hf -> hf.id() != handler.id())
                .sorted(Comparator.comparingInt(EntryPullHandler::count))
                .collect(Collectors.toCollection(LinkedHashSet::new))
                .iterator()
                .next();

        if (minHandler.count() >= minHandler.limit()) {
            return buildHandler();
        }

        minHandler.setCount(minHandler.count() + 1);
        return minHandler;
    }

    @Override
    public void postHandle(EntryPullHandler handler) {
        if (!handlers.contains(handler)) {
            return;
        }

        int count = handler.count() - 1;
        handler.setCount(count);

        handlers.add(handler);
    }

    private EntryPullHandler buildHandler() {
        EventExecutor executor = newEventExecutorGroup(1, "handle").next();
        EntryPullHandler handler = new EntryPullHandler(atomicValue.increment().preValue(), executor, config.getPullHandleThreadLimit(), 1);
        handlers.add(handler);

        return handler;
    }

    @Override
    public void close() {
        if (handlers.isEmpty()) {
            return;
        }

        for (EntryPullHandler handler : handlers) {
            EventExecutor executor = handler.executor();

            if (executor.isShutdown()) {
                continue;
            }
            executor.shutdownGracefully();
        }

        handlers.clear();
    }
}
