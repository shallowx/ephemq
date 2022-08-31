package org.shallow.log.handle.pull;

import io.netty.util.concurrent.EventExecutor;
import org.shallow.internal.atomic.DistributedAtomicInteger;
import org.shallow.internal.config.BrokerConfig;
import org.shallow.logging.InternalLogger;
import org.shallow.logging.InternalLoggerFactory;
import java.util.Comparator;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.stream.Collectors;

import static org.shallow.util.NetworkUtil.newEventExecutorGroup;

public class EntryPullHandlerExecutionChain implements HandlerChain {

    private static final InternalLogger logger = InternalLoggerFactory.getLogger(EntryPullHandlerExecutionChain.class);

    private final BrokerConfig config;
    private final Set<Handler> handlers = new CopyOnWriteArraySet<>();
    private final DistributedAtomicInteger atomicValue = new DistributedAtomicInteger();

    public EntryPullHandlerExecutionChain(BrokerConfig config) {
        this.config = config;
    }

    @Override
    public Handler applyHandler() {
        if (handlers.isEmpty()) {
            return buildHandler();
        }

        Handler minHandler = handlers.stream()
                .sorted(Comparator.comparingInt(Handler::count))
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
    public Handler preHandle(Handler handler) {
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

    private Handler applyHandler(Handler handler) {
        if (handlers.isEmpty()) {
            return buildHandler();
        }

        Handler minHandler = handlers.stream()
                .filter(hf -> hf.id() != handler.id())
                .sorted(Comparator.comparingInt(Handler::count))
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
    public void postHandle(Handler handler) {
        if (!handlers.contains(handler)) {
            return;
        }

        int count = handler.count() - 1;
        handler.setCount(count);

        handlers.add(handler);
    }

    private Handler buildHandler() {
        EventExecutor executor = newEventExecutorGroup(1, "handle").next();
        Handler handler = new Handler(atomicValue.increment().preValue(), executor, config.getPullHandleThreadLimit(), 1);
        handlers.add(handler);
        return handler;
    }

}
