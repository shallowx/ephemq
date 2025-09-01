package org.ephemq.remote.invoke;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.util.AbstractReferenceCounted;
import io.netty.util.Recycler;
import io.netty.util.ReferenceCounted;

import javax.annotation.concurrent.Immutable;

import static org.ephemq.common.util.ObjectUtil.checkPositive;
import static org.ephemq.remote.util.ByteBufUtil.defaultIfNull;

/**
 * The {@code WrappedInvocation} class represents an invocation that wraps a command and its associated data,
 * along with optional feedback and expiration time. It leverages reference counting to manage lifecycle.
 * <p>
 * This class is immutable and provides methods to retrieve command details, data, expiration time,
 * and feedback mechanism. It also implements recycling to reuse instances for memory efficiency.
 */
@Immutable
public final class WrappedInvocation extends AbstractReferenceCounted {
    /**
     * Recycler instance for managing a pool of WrappedInvocation objects,
     * enabling efficient reuse and reducing the overhead of object creation and
     * garbage collection.
     * <p>
     * The RECYCLER provides a mechanism to obtain and recycle WrappedInvocation
     * instances, which are used to encapsulate the details of a specific invocation
     * command, its associated data, expiration time, and feedback mechanism.
     */
    private static final Recycler<WrappedInvocation> RECYCLER = new Recycler<>() {
        @Override
        protected WrappedInvocation newObject(Handle<WrappedInvocation> handle) {
            return new WrappedInvocation(handle);
        }
    };
    /**
     * A handle for recycling instances of WrappedInvocation.
     * <p>
     * This handle is used by the recycler to manage the lifecycle of WrappedInvocation objects,
     * allowing them to be reused, thus reducing garbage collection overhead.
     */
    private final Recycler.Handle<WrappedInvocation> handle;
    /**
     * Feedback mechanism for the {@link WrappedInvocation} class that manages the
     * success or failure responses for an invocation. This is used to handle the
     * result of operations executed within the class.
     */
    private InvokedFeedback<ByteBuf> feedback;
    /**
     * The data buffer associated with the current invocation.
     * This buffer contains the payload or message to be processed
     * during the invocation.
     *
     * @see WrappedInvocation#newInvocation(int, ByteBuf, long, InvokedFeedback)
     * @see WrappedInvocation#data()
     */
    private ByteBuf data;
    /**
     * The timestamp indicating when this invocation will expire.
     * Typically used to determine whether the invocation should still be processed
     * or discarded if it is past its expiration time.
     */
    private long expired;
    /**
     * Represents the command identifier for a wrapped invocation.
     * This integer value is used to specify the type of command being executed.
     */
    private int command;

    /**
     * Constructs a new WrappedInvocation with the provided handle.
     *
     * @param handle the Recycler.Handle instance associated with this WrappedInvocation
     */
    public WrappedInvocation(Recycler.Handle<WrappedInvocation> handle) {
        this.handle = handle;
    }

    /**
     * Creates a new WrappedInvocation with the specified command and data.
     * This is a convenience method that sets the expiration to 0 and feedback to null.
     *
     * @param command the command code for the invocation
     * @param data the data buffer associated with the invocation
     * @return a new WrappedInvocation instance
     */
    public static WrappedInvocation newInvocation(int command, ByteBuf data) {
        return newInvocation(command, data, 0, null);
    }

    /**
     * Creates a new instance of {@link WrappedInvocation}, setting various properties such as command, data, expiration time,
     * and feedback.
     *
     * @param command the command identifier for the invocation, must be a positive integer
     * @param data the data to be associated with the invocation, can be null
     * @param expires the expiration time of the invocation, represented as a long value
     * @param feedback an instance of {@link InvokedFeedback} to handle feedback for this invocation, can be null
     * @return a new instance of {@link WrappedInvocation} with the specified properties
     */
    public static WrappedInvocation newInvocation(int command, ByteBuf data, long expires,
                                                  InvokedFeedback<ByteBuf> feedback) {
        checkPositive(command, "Command");

        final WrappedInvocation invocation = RECYCLER.get();
        invocation.setRefCnt(1);
        invocation.command = command;
        invocation.feedback = feedback;
        invocation.expired = expires;
        invocation.data = defaultIfNull(data, Unpooled.EMPTY_BUFFER);

        return invocation;
    }

    /**
     * Returns the command value associated with this WrappedInvocation.
     *
     * @return the command value as an integer
     */
    public int command() {
        return command;
    }

    /**
     * Returns the ByteBuf that contains the data associated with this WrappedInvocation.
     *
     * @return the ByteBuf containing the data
     */
    public ByteBuf data() {
        return data;
    }

    /**
     * Returns the expiration time of the current invocation.
     *
     * @return the expiration time in milliseconds.
     */
    public long expired() {
        return expired;
    }

    /**
     * Returns the feedback mechanism associated with this invocation.
     *
     * @return an {@code InvokedFeedback<ByteBuf>} instance representing the feedback mechanism
     */
    public InvokedFeedback<ByteBuf> feedback() {
        return feedback;
    }

    /**
     * Deallocates the resources held by this WrappedInvocation instance.
     * <p>
     * The method releases the reference to the underlying ByteBuf if it is not null and
     * sets the data member to null. It also sets the feedback member to null and
     * recycles this instance through the handle to allow for reuse.
     * <p>
     * This method overrides deallocate() from the superclass and is called when
     * the reference count of this instance reaches zero.
     */
    @Override
    protected void deallocate() {
        if (null != data) {
            data.release();
            data = null;
        }
        feedback = null;
        handle.recycle(this);
    }

    /**
     * Retains a reference to the current instance of WrappedInvocation.
     * <p>
     * This method increments the reference count of this instance to ensure
     * that it is not prematurely deallocated. It returns the current instance
     * to facilitate method chaining.
     *
     * @return the current instance of WrappedInvocation with the reference count incremented
     */
    @Override
    public WrappedInvocation retain() {
        super.retain();
        return this;
    }

    /**
     * Retains the reference count of this object by the specified increment.
     *
     * @param increment the amount by which to increase the reference count
     * @return the current instance with the updated reference count
     */
    @Override
    public ReferenceCounted retain(int increment) {
        super.retain(increment);
        return this;
    }

    /**
     * Updates the last accessed time of the data associated with this invocation.
     *
     * @param hint Optional hint object that can provide additional context for the touch operation.
     * @return The current instance of the WrappedInvocation.
     */
    @Override
    public WrappedInvocation touch(Object hint) {
        if (null != data) {
            data.touch(hint);
        }
        return this;
    }

    /**
     * Updates the last access time of this WrappedInvocation to the current time.
     *
     * @return this WrappedInvocation instance.
     */
    @Override
    public WrappedInvocation touch() {
        super.touch();
        return this;
    }

    /**
     * Returns a string representation of the WrappedInvocation instance.
     *
     * @return a string containing information about the handle, command, data, expiration time, and feedback status.
     */
    @Override
    public String toString() {
        return "WrappedInvocation (handle=%s, command=%d, data=%s, expired=%d, feedback=%s)".formatted(handle, command, data, expired, feedback);
    }
}
