package org.meteor.dispatch;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelPromise;
import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.EventExecutorGroup;
import io.netty.util.concurrent.ImmediateEventExecutor;
import io.netty.util.concurrent.Promise;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.IntCollection;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;
import it.unimi.dsi.fastutil.objects.ObjectArraySet;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.WeakHashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.IntConsumer;
import org.meteor.common.logging.InternalLogger;
import org.meteor.common.logging.InternalLoggerFactory;
import org.meteor.common.message.Offset;
import org.meteor.common.util.MessageUtil;
import org.meteor.config.DefaultDispatchConfig;
import org.meteor.exception.DefaultDispatchException;
import org.meteor.ledger.LedgerCursor;
import org.meteor.ledger.LedgerStorage;
import org.meteor.remote.codec.MessagePacket;
import org.meteor.remote.invoke.Command;
import org.meteor.remote.proto.client.MessagePushSignal;
import org.meteor.remote.util.ByteBufUtil;
import org.meteor.remote.util.ProtoBufUtil;

/**
 * DefaultDispatcher is responsible for managing the dispatch of events to various channels.
 * It handles channel operations such as resetting offsets, altering markers, cleaning up resources,
 * and managing the state of channel handlers.
 */
public class DefaultDispatcher {

    private static final InternalLogger logger = InternalLoggerFactory.getLogger(DefaultDispatcher.class);
    /**
     * Represents the ledger identifier for the DefaultDispatcher.
     */
    private final int ledger;
    /**
     * Represents the topic associated with the {@link DefaultDispatcher}.
     * This variable holds the name of the topic for which the dispatcher is responsible.
     * It is a final and immutable string, ensuring the topic name remains consistent
     * throughout the lifecycle of the dispatcher instance.
     */
    private final String topic;
    /**
     * The LedgerStorage instance used by the DefaultDispatcher to handle data
     * storage and retrieval operations related to ledger entries and logs.
     * This is a final variable, ensuring that the reference to the LedgerStorage
     * instance cannot be changed once it is assigned.
     */
    private final LedgerStorage storage;
    /**
     * The maximum number of entities a user can follow.
     * This variable defines the cap on the number of follow relationships
     * that can be established by a single user to other users or entities within
     * the application.
     */
    private final int followLimit;
    /**
     * The maximum number of attempts allowed for pursuing a specific operation or goal.
     */
    private final int pursueLimit;
    /**
     * Specifies the maximum limit for alignment operations in the dispatcher.
     * It dictates the threshold up to which the components will attempt to align
     * their states or data before initiating further operations like follow or pursue.
     */
    private final int alignLimit;
    /**
     * The duration, in milliseconds, before a pursue action times out.
     * This value determines the maximum amount of time the system will
     * spend on a pursue operation before considering it as failed or
     * canceled.
     */
    private final long pursueTimeoutMilliseconds;
    /**
     * Represents the maximum permissible load a system or component can handle.
     * This value is used to ensure that operations do not exceed safe operational limits.
     */
    private final int loadLimit;
    /**
     * A consumer that performs operations on an integer value.
     * This final variable is meant to be set once and not modified thereafter.
     * It consumes an integer input as part of its operation, which is defined by the implementation of the IntConsumer.
     */
    private final IntConsumer counter;
    /**
     * Array of EventExecutor instances used for executing tasks associated with event handling.
     * These executors are responsible for managing the concurrency aspects of events being dispatched
     * within the DefaultDispatcher.
     */
    private final EventExecutor[] executors;
    /**
     * A thread-safe list of DefaultHandler instances used within the DefaultDispatcher class
     * to manage and dispatch events. The list is backed by a CopyOnWriteArrayList to ensure
     * safe iteration and modification in concurrent environments.
     */
    private final List<DefaultHandler> dispatchHandlers = new CopyOnWriteArrayList<>();
    /**
     * A map that holds `DefaultHandler` instances as keys with their respective integer values.
     * This map uses weak references for its keys, so entries will be removed when the key is no longer in use.
     * Typically used to keep track of dispatch handlers in a weakly referenced manner to prevent memory leaks.
     */
    private final WeakHashMap<DefaultHandler, Integer> weakHandlers = new WeakHashMap<>();
    /**
     * A thread-safe map that associates a {@link Channel} with its corresponding {@link DefaultHandler}.
     * This map is used to manage and track the handlers assigned to different channels within the {@link DefaultDispatcher}.
     */
    private final ConcurrentMap<Channel, DefaultHandler> channelHandlers = new ConcurrentHashMap<>();
    /**
     * A thread-safe boolean flag indicating the current state.
     * <p>
     * This AtomicBoolean is initialized to {@code true} and can be used to manage
     * concurrency or state changes across multiple threads safely.
     */
    private final AtomicBoolean state = new AtomicBoolean(true);

    /**
     * Constructs a DefaultDispatcher with the specified parameters.
     *
     * @param ledger                     the ledger identifier
     * @param topic                      the topic name
     * @param storage                    the ledger storage instance
     * @param config                     the default dispatch configuration
     * @param group                      the event executor group
     * @param dispatchCounter            the dispatch counter as an IntConsumer
     */
    public DefaultDispatcher(int ledger, String topic, LedgerStorage storage, DefaultDispatchConfig config,
                             EventExecutorGroup group, IntConsumer dispatchCounter) {
        this.ledger = ledger;
        this.topic = topic;
        this.storage = storage;
        this.followLimit = config.getDispatchEntryFollowLimit();
        this.pursueLimit = config.getDispatchEntryPursueLimit();
        this.alignLimit = config.getDispatchEntryAlignLimit();
        this.pursueTimeoutMilliseconds = config.getDispatchEntryPursueTimeoutMilliseconds();
        this.loadLimit = config.getDispatchEntryLoadLimit();
        this.counter = dispatchCounter;

        List<EventExecutor> eventExecutors = new ArrayList<>();
        group.forEach(eventExecutors::add);
        Collections.shuffle(eventExecutors);
        this.executors = eventExecutors.toArray(new EventExecutor[0]);
    }

    /**
     * Returns the number of channel handlers currently managed by the dispatcher.
     *
     * @return the number of channel handlers.
     */
    public int channelCount() {
        return channelHandlers.size();
    }

    /**
     * Returns the EventExecutor assigned to handle the specified channel.
     *
     * @param channel the channel for which to retrieve the executor
     * @return the EventExecutor assigned to handle the specified channel
     */
    private EventExecutor channelExecutor(Channel channel) {
        return executors[(channel.hashCode() & 0x7fffffff) % executors.length];
    }

    /**
     * Resets the state of a specified channel to the given offset and applies provided markers.
     *
     * @param channel the channel to reset.
     * @param resetOffset the offset to reset the channel to.
     * @param wholeMarkers the collection of markers to be applied to the reset state.
     * @param promise the promise representing the outcome of the reset operation which on success carries the size of markers applied.
     */
    public void reset(Channel channel, Offset resetOffset, IntCollection wholeMarkers, Promise<Integer> promise) {
        try {
            EventExecutor executor = channelExecutor(channel);
            if (executor.inEventLoop()) {
                doReset(channel, resetOffset, wholeMarkers, promise);
            } else {
                executor.execute(() -> doReset(channel, resetOffset, wholeMarkers, promise));
            }
        } catch (Throwable t) {
            promise.tryFailure(t);
        }
    }

    /**
     * Resets the specified channel with the given reset offset and a collection of markers, and fulfills a promise upon completion.
     *
     * @param channel The channel to reset.
     * @param resetOffset The offset to reset the channel to.
     * @param wholeMarkers A collection of markers to be processed during the reset.
     * @param promise The promise that will be fulfilled with the result of the reset operation.
     */
    public void doReset(Channel channel, Offset resetOffset, IntCollection wholeMarkers, Promise<Integer> promise) {
        try {
            checkActive();
            if (wholeMarkers.isEmpty()) {
                Promise<Boolean> cleanPromise = ImmediateEventExecutor.INSTANCE.newPromise();
                cleanPromise.addListener(future -> {
                    if (future.isSuccess()) {
                        promise.trySuccess(0);
                    } else {
                        promise.tryFailure(future.cause());
                    }
                });
                doClean(channel, cleanPromise);
                return;
            }

            DefaultHandler handler = allocateHandler(channel);
            ConcurrentMap<Channel, DefaultSynchronization> channelSubscriptionMap = handler.getSubscriptionChannels();
            DefaultSynchronization oldSubscription = channelSubscriptionMap.get(channel);
            DefaultSynchronization newSubscription =
                    new DefaultSynchronization(channel, handler, new IntOpenHashSet(wholeMarkers));
            handler.getDispatchExecutor().execute(() -> {
                Int2ObjectMap<Set<DefaultSynchronization>> markerSubscriptionMap = handler.getSubscriptionMarkers();
                if (oldSubscription != null) {
                    oldSubscription.getMarkers().forEach((int marker) -> detachMarker(markerSubscriptionMap, marker, newSubscription));
                }

                wholeMarkers.forEach((int marker) -> attachMarker(markerSubscriptionMap, marker, newSubscription));
                if (handler.getFollowCursor() == null) {
                    handler.setFollowOffset(storage.currentOffset());
                    handler.setFollowCursor(storage.cursor(handler.getFollowOffset()));
                    dispatchHandlers.add(handler);
                    touchDispatch(handler);
                }

                Offset dispatchOffset;
                if (resetOffset != null) {
                    Offset earlyOffset = storage.headOffset();
                    dispatchOffset = earlyOffset.after(resetOffset) ? earlyOffset : resetOffset;
                } else {
                    dispatchOffset = storage.currentOffset();
                }

                newSubscription.setDispatchOffset(dispatchOffset);
                if (dispatchOffset.before(handler.getFollowOffset())) {
                    PursueTask<DefaultSynchronization> task =
                            new PursueTask<>(newSubscription, storage.cursor(dispatchOffset), dispatchOffset);
                    submitPursue(task);
                } else {
                    newSubscription.setFollowed(true);
                }
            });

            channelSubscriptionMap.put(channel, newSubscription);
            channelHandlers.putIfAbsent(channel, handler);
            promise.trySuccess(newSubscription.getMarkers().size());
        } catch (Throwable t) {
            promise.tryFailure(t);
        }
    }

    /**
     * Alters the state of a given channel by appending and deleting marker sets.
     *
     * @param channel The channel whose state is to be altered.
     * @param appendMarkers The markers to be appended to the channel.
     * @param deleteMarkers The markers to be deleted from the channel.
     * @param promise A promise to indicate the success or failure of the operation.
     */
    public void alter(Channel channel, IntCollection appendMarkers, IntCollection deleteMarkers, Promise<Integer> promise) {
        try {
            EventExecutor executor = channelExecutor(channel);
            if (executor.inEventLoop()) {
                doAlter(channel, appendMarkers, deleteMarkers, promise);
            } else {
                executor.execute(() -> doAlter(channel, appendMarkers, deleteMarkers, promise));
            }
        } catch (Throwable t) {
            promise.tryFailure(t);
        }
    }

    /**
     * Alters the subscription markers for the specified channel by adding and removing markers.
     *
     * @param channel        The channel whose subscription markers are to be altered.
     * @param appendMarkers  A collection of markers to be added to the subscription.
     * @param deleteMarkers  A collection of markers to be removed from the subscription.
     * @param promise        A promise that will be completed when the operation finishes,
     *                       indicating the number of markers remaining in the subscription.
     */
    private void doAlter(Channel channel, IntCollection appendMarkers, IntCollection deleteMarkers, Promise<Integer> promise) {
        try {
            checkActive();
            DefaultHandler handler = channelHandlers.get(channel);
            ConcurrentMap<Channel, DefaultSynchronization> channelSubscriptionMap =
                    handler == null ? null : handler.getSubscriptionChannels();
            DefaultSynchronization subscription =
                    channelSubscriptionMap == null ? null : channelSubscriptionMap.get(channel);
            if (subscription == null) {
                promise.tryFailure(
                        new DefaultDispatchException("Channel<%s> alter is invalid".formatted(channel.toString())));
                return;
            }

            handler.getDispatchExecutor().execute(() -> {
                Int2ObjectMap<Set<DefaultSynchronization>> markerSubscriptionMap = handler.getSubscriptionMarkers();
                deleteMarkers.forEach((int marker) -> detachMarker(markerSubscriptionMap, marker, subscription));
                appendMarkers.forEach((int marker) -> attachMarker(markerSubscriptionMap, marker, subscription));

                if (markerSubscriptionMap.isEmpty()) {
                    dispatchHandlers.remove(handler);
                    handler.setFollowCursor(null);
                    handler.setFollowOffset(null);
                }
            });

            IntSet markers = subscription.getMarkers();
            markers.removeAll(deleteMarkers);
            markers.addAll(appendMarkers);
            if (markers.isEmpty()) {
                channelSubscriptionMap.remove(channel);
                channelHandlers.remove(channel);
            }
            promise.trySuccess(markers.size());
        } catch (Throwable t) {
            promise.tryFailure(t);
        }
    }

    /**
     * Cleans a specific channel by performing necessary clean-up operations.
     * The operation is executed immediately if the current thread is in the event loop,
     * otherwise it is submitted to be executed in the event loop.
     *
     * @param channel the channel to be cleaned
     * @param promise the promise to be completed with the result of the clean operation
     */
    public void clean(Channel channel, Promise<Boolean> promise) {
        try {
            EventExecutor executor = channelExecutor(channel);
            if (executor.inEventLoop()) {
                doClean(channel, promise);
            } else {
                executor.execute(() -> doClean(channel, promise));
            }
        } catch (Throwable t) {
            promise.tryFailure(t);
        }
    }

    /**
     * Cleans up resources associated with the specified channel and completes the provided promise accordingly.
     * This includes removing various markers and handlers linked to the channel.
     *
     * @param channel the Channel to be cleaned
     * @param promise the Promise that will be completed with the result of the clean operation
     */
    private void doClean(Channel channel, Promise<Boolean> promise) {
        try {
            checkActive();

            DefaultHandler handler = channelHandlers.get(channel);
            ConcurrentMap<Channel, DefaultSynchronization> channelSubscriptionMap =
                    handler == null ? null : handler.getSubscriptionChannels();
            DefaultSynchronization subscription =
                    channelSubscriptionMap == null ? null : channelSubscriptionMap.get(channel);
            if (subscription == null) {
                promise.trySuccess(false);
                return;
            }

            handler.getDispatchExecutor().execute(() -> {
                Int2ObjectMap<Set<DefaultSynchronization>> markerSubscriptionMap = handler.getSubscriptionMarkers();
                subscription.getMarkers().forEach((int marker) -> detachMarker(markerSubscriptionMap, marker, subscription));
                if (markerSubscriptionMap.isEmpty()) {
                    dispatchHandlers.remove(handler);
                    handler.setFollowOffset(null);
                    handler.setFollowCursor(null);
                }
            });

            channelSubscriptionMap.remove(channel);
            channelHandlers.remove(channel);
            promise.trySuccess(true);
        } catch (Throwable t) {
            promise.tryFailure(t);
        }
    }

    /**
     * Attempts to dispatch work to the specified handler. If the handler has not been triggered yet,
     * it sets the triggered status and schedules the dispatch execution.
     *
     * @param handler The handler to which the dispatch work will be delegated.
     */
    private void touchDispatch(DefaultHandler handler) {
        if (handler.getTriggered().compareAndSet(false, true)) {
            try {
                handler.getDispatchExecutor().execute(() -> doDispatch(handler));
            } catch (Throwable t) {
                if (logger.isErrorEnabled()) {
                    logger.error("Dispatch submit failed, handler[{}]", handler, t);
                }
            }
        }
    }

    /**
     * Dispatches messages for the given handler by iterating through ledger entries.
     *
     * @param handler the handler for which dispatching should occur
     */
    private void doDispatch(DefaultHandler handler) {
        LedgerCursor cursor = handler.getFollowCursor();
        if (cursor == null) {
            handler.getTriggered().set(false);
            return;
        }

        Int2ObjectMap<Set<DefaultSynchronization>> markerSubscriptionMap = handler.getSubscriptionMarkers();
        Offset lastOffset = handler.getFollowOffset();
        int count = 0;
        try {
            int runTimes = 0;
            ByteBuf entry;
            while ((entry = cursor.next()) != null) {
                runTimes++;
                ByteBuf payload = null;
                try {
                    Offset offset = MessageUtil.getOffset(entry);
                    if (!offset.after(lastOffset)) {
                        if (runTimes > followLimit) {
                            break;
                        }
                        continue;
                    }

                    lastOffset = offset;
                    int marker = MessageUtil.getMarker(entry);
                    Set<DefaultSynchronization> subscriptions = markerSubscriptionMap.get(marker);
                    if (subscriptions == null) {
                        if (runTimes > followLimit) {
                            break;
                        }
                        continue;
                    }

                    for (DefaultSynchronization subscription : subscriptions) {
                        if (!subscription.isFollowed()) {
                            continue;
                        }
                        Channel channel = subscription.getChannel();
                        if (!channel.isActive()) {
                            continue;
                        }

                        if (!offset.after(subscription.getDispatchOffset())) {
                            continue;
                        }

                        subscription.setDispatchOffset(offset);
                        if (payload == null) {
                            payload = createPayload(marker, offset, entry, channel.alloc());
                        }

                        count++;
                        if (channel.isWritable()) {
                            channel.writeAndFlush(payload.retainedSlice(), channel.voidPromise());
                        } else {
                            subscription.setFollowed(false);
                            PursueTask<DefaultSynchronization> task =
                                    new PursueTask<>(subscription, cursor.copy(), offset);
                            channel.writeAndFlush(payload.retainedSlice(), delayPursue(task));
                        }
                    }
                } catch (Throwable t) {
                    if (logger.isErrorEnabled()) {
                        logger.error("Dispatch failed, handler[{}] lastOffset[{}]", handler, lastOffset, t);
                    }
                } finally {
                    ByteBufUtil.release(entry);
                    ByteBufUtil.release(payload);
                }
                if (runTimes > followLimit) {
                    break;
                }
            }
        } catch (Throwable t) {
            if (logger.isErrorEnabled()) {
                logger.error("Dispatch execute failed, handler[{}] lastOffset[{}]", handler, lastOffset, t);
            }
        } finally {
            handler.getTriggered().set(false);
        }

        handler.setFollowOffset(lastOffset);
        compute(count);
        if (cursor.hashNext()) {
            touchDispatch(handler);
        }
    }

    /**
     * Processes the given count by accepting it into a counter and handles any
     * errors that occur during this process. Errors are logged if error logging
     * is enabled.
     *
     * @param count the count value to be processed. If the count is greater than 0,
     *              it will be accepted by the counter. Otherwise, no processing
     *              occurs.
     */
    private void compute(int count) {
        if (count > 0) {
            try {
                counter.accept(count);
            } catch (Throwable t) {
                if (logger.isErrorEnabled()) {
                    logger.error("Record count failed, ledger[{}] topic[{}]", ledger, topic, t);
                }
            }
        }
    }

    /**
     * Creates a {@link ChannelPromise} for the given {@link PursueTask} and sets up a listener that will
     * submit the task for pursuit if the associated channel is active.
     *
     * @param task the pursue task containing the subscription and other relevant information.
     * @return a ChannelPromise which will be completed once the task is either submitted for pursuit or fails.
     */
    private ChannelPromise delayPursue(PursueTask<DefaultSynchronization> task) {
        ChannelPromise promise = task.getSubscription().getChannel().newPromise();
        promise.addListener((ChannelFutureListener) f -> {
            if (f.channel().isActive()) {
                submitPursue(task);
            }
        });
        return promise;
    }

    /**
     * Submits a pursue task to the appropriate channel executor.
     *
     * @param task the pursue task to be submitted, containing the subscription and cursor details.
     */
    private void submitPursue(PursueTask<DefaultSynchronization> task) {
        try {
            channelExecutor(task.getSubscription().getChannel()).execute(() -> {
                doPursue(task);
            });
        } catch (Throwable t) {
            task.getSubscription().setFollowed(true);
            if (logger.isErrorEnabled()) {
                logger.error("Submit failed, task[{}]", task);
            }
        }
    }

    /**
     * Executes a pursue task that continues processing from a specified offset.
     *
     * @param task The PursueTask containing the details of the subscription and cursor to process.
     */
    private void doPursue(PursueTask<DefaultSynchronization> task) {
        DefaultSynchronization subscription = task.getSubscription();
        Channel channel = subscription.getChannel();
        AbstractHandler<DefaultSynchronization, DefaultHandler> handler = subscription.getHandler();

        if (!channel.isActive() || subscription != handler.getSubscriptionChannels().get(channel)) {
            return;
        }

        if (System.currentTimeMillis() - task.getPursueTimeMillis() > pursueTimeoutMilliseconds) {
            if (logger.isWarnEnabled()) {
                logger.warn("Giving up pursue task[{}]", task);
            }
            submitFollow(task);
            return;
        }

        IntSet markers = subscription.getMarkers();
        LedgerCursor cursor = task.getCursor();
        boolean finished = true;
        Offset lastOffset = task.getPursueOffset();
        int count = 0;
        try {
            int runTimes = 0;
            ByteBuf entry;
            while ((entry = cursor.next()) != null) {
                runTimes++;
                ByteBuf payload = null;
                try {
                    Offset offset = MessageUtil.getOffset(entry);
                    if (!offset.after(lastOffset)) {
                        if (runTimes > pursueLimit) {
                            finished = false;
                            break;
                        }
                        continue;
                    }
                    lastOffset = offset;
                    int marker = MessageUtil.getMarker(entry);
                    if (!markers.contains(marker)) {
                        if (runTimes > pursueLimit) {
                            finished = false;
                            break;
                        }
                        continue;
                    }

                    subscription.setDispatchOffset(offset);
                    payload = createPayload(marker, offset, entry, channel.alloc());
                    count++;
                    if (channel.isWritable()) {
                        channel.writeAndFlush(payload.retainedSlice(), channel.voidPromise());
                    } else {
                        task.setPursueOffset(lastOffset);
                        compute(count);
                        channel.writeAndFlush(payload.retainedSlice(), delayPursue(task));
                        return;
                    }
                } catch (Throwable t) {
                    if (logger.isErrorEnabled()) {
                        logger.error("Pursue failed, task[{}] lastOffset[{}]", task, lastOffset, t);
                    }
                } finally {
                    ByteBufUtil.release(entry);
                    ByteBufUtil.release(payload);
                }

                if (runTimes > pursueLimit) {
                    finished = false;
                    break;
                }
            }
        } catch (Throwable t) {
            if (logger.isErrorEnabled()) {
                logger.error("Pursue execute failed, task[{}] lastOffset[{}]", task, lastOffset, t);
            }
        }

        task.setPursueOffset(lastOffset);
        compute(count);
        Offset alignOffset = handler.getFollowOffset();
        if (finished || (alignOffset != null && !lastOffset.before(alignOffset))) {
            submitAlign(task);
        } else {
            submitPursue(task);
        }
    }

    /**
     * Submits a follow task for execution on the dispatch executor. If an exception occurs
     * during the execution, the subscription is marked as followed regardless, and an error
     * message is logged.
     *
     * @param task the pursue task that encapsulates the subscription and cursor information.
     */
    private void submitFollow(PursueTask<DefaultSynchronization> task) {
        DefaultSynchronization subscription = task.getSubscription();
        try {
            subscription.getHandler().getDispatchExecutor().execute(() -> {
                subscription.setFollowed(true);
            });
        } catch (Throwable t) {
            subscription.setFollowed(true);
            if (logger.isErrorEnabled()) {
                logger.error("Submit failed, task[{}]", task);
            }
        }
    }

    /**
     * Submits a task for alignment. This method retrieves the associated subscription from the task
     * and attempts to execute the alignment process on the dispatch executor associated with the
     * subscription's handler. If an error occurs during this process, appropriate error handling
     * is performed.
     *
     * @param task the task to be aligned, encapsulated in a {@link PursueTask} object.
     */
    private void submitAlign(PursueTask<DefaultSynchronization> task) {
        DefaultSynchronization subscription = task.getSubscription();
        try {
            subscription.getHandler().getDispatchExecutor().execute(() -> {
                doAlign(task);
            });
        } catch (Throwable t) {
            subscription.setFollowed(true);
            if (logger.isErrorEnabled()) {
                logger.error("Submit failed, task[{}]", task);
            }
        }
    }

    /**
     * Aligns the given pursue task by synchronizing it with the appropriate offsets and markers.
     *
     * @param task the pursue task to be aligned, which contains the subscription and cursor needed for alignment.
     */
    private void doAlign(PursueTask<DefaultSynchronization> task) {
        DefaultSynchronization subscription = task.getSubscription();
        Channel channel = subscription.getChannel();
        AbstractHandler<DefaultSynchronization, DefaultHandler> handler = subscription.getHandler();

        if (!channel.isActive() || subscription != handler.getSubscriptionChannels().get(channel)) {
            return;
        }

        Offset alignOffset = handler.getFollowOffset();
        Offset lastOffset = task.getPursueOffset();
        if (!lastOffset.before(alignOffset)) {
            subscription.setFollowed(true);
            return;
        }

        IntSet markers = subscription.getMarkers();
        LedgerCursor cursor = task.getCursor();
        boolean finished = true;
        int count = 0;
        try {
            int runTimes = 0;
            ByteBuf entry;
            while ((entry = cursor.next()) != null) {
                runTimes++;
                ByteBuf payload = null;
                try {
                    Offset offset = MessageUtil.getOffset(entry);
                    if (!offset.after(lastOffset)) {
                        if (runTimes > alignLimit) {
                            finished = false;
                            break;
                        }
                        continue;
                    }

                    if (offset.after(alignOffset)) {
                        subscription.setFollowed(true);
                        compute(count);
                        return;
                    }

                    lastOffset = offset;
                    int marker = MessageUtil.getMarker(entry);
                    if (!markers.contains(marker)) {
                        if (runTimes > pursueLimit) {
                            finished = false;
                            break;
                        }
                        continue;
                    }

                    subscription.setDispatchOffset(offset);
                    payload = createPayload(marker, offset, entry, channel.alloc());
                    count++;
                    if (channel.isWritable()) {
                        channel.writeAndFlush(payload.retainedSlice(), channel.voidPromise());
                    } else {
                        task.setPursueOffset(lastOffset);
                        compute(count);
                        channel.writeAndFlush(payload.retainedSlice(), delayPursue(task));
                        return;
                    }

                } catch (Throwable t) {
                    if (logger.isErrorEnabled()) {
                        logger.error("Switch to pursue, channel is full, task[{}]", task);
                    }
                } finally {
                    ByteBufUtil.release(entry);
                    ByteBufUtil.release(payload);
                }

                if (runTimes > alignLimit) {
                    finished = false;
                    break;
                }
            }
        } catch (Throwable t) {
            if (logger.isErrorEnabled()) {
                logger.error("Pursue execute failed, task[{}] lastOffset[{}]", task, lastOffset, t);
            }
        }

        if (finished) {
            subscription.setFollowed(true);
            compute(count);
            return;
        }

        task.setPursueOffset(lastOffset);
        compute(count);
        submitPursue(task);
    }

    /**
     * Creates a payload byte buffer by combining a message push signal and an existing entry buffer.
     * The payload contains metadata and the actual data to be dispatched.
     *
     * @param marker An integer marker representing a specific state or position.
     * @param offset The offset object containing epoch and index.
     * @param entry The buffer containing the entry data.
     * @param alloc The byte buffer allocator.
     * @return A combined byte buffer ready for dispatch.
     * @throws DefaultDispatchException if an error occurs during payload creation.
     */
    private ByteBuf createPayload(int marker, Offset offset, ByteBuf entry, ByteBufAllocator alloc) {
        ByteBuf buf = null;
        try {
            MessagePushSignal signal = MessagePushSignal.newBuilder()
                    .setEpoch(offset.getEpoch())
                    .setIndex(offset.getIndex())
                    .setMarker(marker)
                    .setLedger(ledger)
                    .build();
            int signalLength = ProtoBufUtil.protoLength(signal);
            int contentLength = entry.readableBytes() - 16;

            buf = alloc.ioBuffer(MessagePacket.HEADER_LENGTH + signalLength);
            buf.writeByte(MessagePacket.MAGIC_NUMBER);
            buf.writeMedium(MessagePacket.HEADER_LENGTH + signalLength + contentLength);
            buf.writeInt(Command.Client.PUSH_MESSAGE);
            buf.writeLong(0L);

            ProtoBufUtil.writeProto(buf, signal);
            buf = Unpooled.wrappedUnmodifiableBuffer(buf, entry.retainedSlice(entry.readerIndex() + 16, contentLength));

            return buf;
        } catch (Throwable t) {
            ByteBufUtil.release(buf);
            throw new DefaultDispatchException(
                    "Build payload error, ledger[%d] topic[%s] offset[%s] length[%d]".formatted(ledger, t, offset,
                            entry.readableBytes()));
        }
    }

    /**
     * Dispatches events to registered handlers if there are any.
     * For each handler in the dispatchHandlers list, if the handler has a follow cursor,
     * it calls the touchDispatch method to handle the dispatch for that handler.
     */
    public void dispatch() {
        if (dispatchHandlers.isEmpty()) {
            return;
        }
        for (DefaultHandler handler : dispatchHandlers) {
            if (handler.getFollowCursor() != null) {
                touchDispatch(handler);
            }
        }
    }

    /**
     * Detaches a given subscription from a specific marker in the marker subscription map.
     *
     * @param markerSubscriptionMap the map that associates markers with their respective sets of subscriptions
     * @param marker the marker from which the subscription will be detached
     * @param subscription the subscription to detach from the specified marker
     */
    private void detachMarker(Int2ObjectMap<Set<DefaultSynchronization>> markerSubscriptionMap, int marker,
                              DefaultSynchronization subscription) {
        Set<DefaultSynchronization> subscriptions = markerSubscriptionMap.get(marker);
        if (subscriptions != null) {
            subscriptions.remove(subscription);
            if (subscriptions.isEmpty()) {
                markerSubscriptionMap.remove(marker);
            }
        }
    }

    /**
     * Attaches a {@link DefaultSynchronization} subscription to a specified marker in the marker subscription map.
     *
     * @param markerSubscriptionMap the map holding sets of subscriptions for each marker
     * @param marker the marker to which the subscription should be attached
     * @param subscription the subscription to attach to the specified marker
     */
    private void attachMarker(Int2ObjectMap<Set<DefaultSynchronization>> markerSubscriptionMap, int marker,
                              DefaultSynchronization subscription) {
        markerSubscriptionMap.computeIfAbsent(marker, k -> new ObjectArraySet<>()).add(subscription);
    }

    /**
     * Ensures that the dispatch handler is active before proceeding.
     * <p>
     * This method checks the current state of the dispatcher using the
     * {@link #isActive()} method. If the dispatcher is not active, it throws
     * a {@link DefaultDispatchException} to prevent further processing.
     * </p>
     *
     * @throws DefaultDispatchException if the dispatcher is inactive
     */
    private void checkActive() {
        if (!isActive()) {
            throw new DefaultDispatchException("Dispatch handler is inactive");
        }
    }

    /**
     * Checks if the dispatcher is currently active.
     *
     * @return {@code true} if the dispatcher is active, otherwise {@code false}
     */
    public boolean isActive() {
        return state.get();
    }

    /**
     * Closes the dispatcher by cleaning up associated channels and their markers.
     *
     * @param promise A promise that will be fulfilled with a map of channels and their markers upon successful closure,
     *                or with an exception if an error occurs.
     * @return A Future representing the final outcome of the closure operation, containing the map of channels
     *         and their markers if successful.
     */
    public Future<Map<Channel, IntSet>> close(Promise<Map<Channel, IntSet>> promise) {
        Promise<Map<Channel, IntSet>> result = promise != null ? promise : ImmediateEventExecutor.INSTANCE.newPromise();
        if (state.compareAndSet(true, false)) {
            Map<Channel, IntSet> channelMarkers = new ConcurrentHashMap<>();
            AtomicInteger count = new AtomicInteger(executors.length);
            for (EventExecutor executor : executors) {
                try {
                    executor.submit(() -> {
                        for (Channel channel : channelHandlers.keySet()) {
                            if (channelExecutor(channel).inEventLoop()) {
                                AbstractHandler<DefaultSynchronization, DefaultHandler> handler =
                                        channelHandlers.get(channel);
                                ConcurrentMap<Channel, DefaultSynchronization> channelSubscriptionMap =
                                        handler == null ? null : handler.getSubscriptionChannels();
                                DefaultSynchronization
                                        subscription =
                                        channelSubscriptionMap == null ? null : channelSubscriptionMap.get(channel);
                                if (subscription != null) {
                                    channelMarkers.put(channel, subscription.getMarkers());
                                }
                                doClean(channel, ImmediateEventExecutor.INSTANCE.newPromise());
                            }
                        }

                        if (count.decrementAndGet() == 0) {
                            result.trySuccess(channelMarkers);
                        }
                    });
                } catch (Throwable t) {
                    result.tryFailure(t);
                }
            }

        } else {
            result.trySuccess(null);
        }
        return result;
    }

    /**
     * Allocates and returns a DefaultHandler instance for the specified channel.
     * This method first checks if a handler is already assigned to the channel.
     * If not, it tries to allocate a handler with a balanced load among weak handlers.
     *
     * @param channel the channel for which the DefaultHandler is to be allocated.
     * @return DefaultHandler the allocated handler for the specified channel.
     */
    private DefaultHandler allocateHandler(Channel channel) {
        DefaultHandler result = channelHandlers.get(channel);
        if (result != null) {
            return result;
        }
        ThreadLocalRandom random = ThreadLocalRandom.current();
        int middleLimit = loadLimit >> 1;
        synchronized (weakHandlers) {
            if (weakHandlers.isEmpty()) {
                return DefaultHandler.INSTANCE.newHandler(weakHandlers, executors);
            }

            Map<DefaultHandler, Integer> selectHandlers = new HashMap<>();
            int randomBound = 0;
            for (DefaultHandler handler : weakHandlers.keySet()) {
                int channelCount = handler.getSubscriptionChannels().size();
                if (channelCount >= loadLimit) {
                    continue;
                }

                if (channelCount >= middleLimit) {
                    randomBound += loadLimit - channelCount;
                    selectHandlers.put(handler, channelCount);
                } else if (result == null || result.getSubscriptionChannels().size() < channelCount) {
                    result = handler;
                }
            }

            if (selectHandlers.isEmpty() || randomBound == 0) {
                return result != null ? result : DefaultHandler.INSTANCE.newHandler(weakHandlers, executors);
            }

            int index = random.nextInt(randomBound);
            int count = 0;
            for (Map.Entry<DefaultHandler, Integer> entry : selectHandlers.entrySet()) {
                count += loadLimit - entry.getValue();
                if (index < count) {
                    return entry.getKey();
                }
            }
            return result != null ? result : DefaultHandler.INSTANCE.newHandler(weakHandlers, executors);
        }
    }
}
