package org.meteor.remote.codec;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufConvertible;
import io.netty.buffer.Unpooled;
import io.netty.util.AbstractReferenceCounted;
import io.netty.util.Recycler;
import io.netty.util.ReferenceCounted;

import javax.annotation.concurrent.Immutable;

import static org.meteor.remote.util.ByteBufUtil.defaultIfNull;

/**
 * This class represents a message packet with a specified feedback, command, and body.
 * It is immutable and extends {@link AbstractReferenceCounted}. It implements
 * {@link Comparable} and {@link ByteBufConvertible}.
 * <p>
 * Each {@code MessagePacket} contains a static recycler for object pooling,
 * a magic number, header length, maximum frame length, and maximum body length.
 * These constants help to manage packet structure and memory.
 * <p>
 * Methods are provided to create new packets, access the packet's properties,
 * and manage the reference count and deallocation of the packet.
 */
@Immutable
public final class MessagePacket extends AbstractReferenceCounted
        implements Comparable<MessagePacket>, ByteBufConvertible {

    /**
     * Represents the length of the header for a MessagePacket.
     * The value is set to 16 bytes.
     */
    public static final byte HEADER_LENGTH = 16;
    /**
     * The maximum allowable length for a frame in the MessagePacket.
     */
    public static final int MAX_FRAME_LENGTH = 4194316;
    /**
     * Represents the maximum allowed length for the body of a message packet.
     * The value is calculated by subtracting the header length from the
     * maximum frame length.
     */
    public static final int MAX_BODY_LENGTH = MAX_FRAME_LENGTH - HEADER_LENGTH;
    /**
     * A constant representing a special value (0x2c) used within the MessagePacket class.
     * It is typically employed in the protocol for identifying or processing packets.
     */
    public static final byte MAGIC_NUMBER = (byte) 0x2c;

    /**
     * A recycler for instances of {@code MessagePacket}. It provides a mechanism to reuse
     * {@code MessagePacket} objects in order to enhance performance and reduce garbage
     * collection overhead.
     * <p>
     * The recycler overrides the method to provide a new instance of {@code MessagePacket} when none are available
     * in the pool.
     */
    private static final Recycler<MessagePacket> RECYCLER = new Recycler<>() {
        @Override
        protected MessagePacket newObject(Handle<MessagePacket> handle) {
            return new MessagePacket(handle);
        }
    };

    /**
     * A handle to a recycled {@link MessagePacket} object.
     * <p>
     * This handle is provided by a Recycler to manage instances of
     * {@link MessagePacket} with potential reuse and memory efficiency.
     * <p>
     * Handles are used to claim and release {@link MessagePacket}
     * instances back to the Recycler pool.
     */
    private final Recycler.Handle<MessagePacket> handle;
    /**
     * The body of the message packet containing the payload data.
     */
    private ByteBuf body;

    /**
     * A unique identifier provided in the packet for tracking or reference purposes.
     */
    private long feedback;
    /**
     * Represents the command identifier for this MessagePacket.
     * This field holds the integer value that specifies the type
     * of command being encapsulated within the packet.
     */
    private int command;

    /**
     * Constructs a new MessagePacket with the specified {@link Recycler.Handle}.
     *
     * @param handle the Recycler.Handle<MessagePacket> instance used to manage this MessagePacket object's lifecycle
     */
    private MessagePacket(Recycler.Handle<MessagePacket> handle) {
        this.handle = handle;
    }

    /**
     * Creates a new instance of {@code MessagePacket} with the given feedback, command, and body.
     *
     * @param feedback the feedback identifier for the message packet
     * @param command the command type of the message packet
     * @param body the body of the message packet, can be null which results in an empty buffer
     * @return a newly allocated {@code MessagePacket} with the specified properties
     */
    public static MessagePacket newPacket(long feedback, int command, ByteBuf body) {
        final MessagePacket packet = RECYCLER.get();
        packet.setRefCnt(1);
        packet.feedback = feedback;
        packet.command = command;
        packet.body = defaultIfNull(body, Unpooled.EMPTY_BUFFER);

        return packet;
    }

    /**
     * Retrieves the feedback identifier of the message packet.
     *
     * @return the feedback identifier as a long value
     */
    public long feedback() {
        return feedback;
    }

    /**
     * Returns the body of this MessagePacket as a ByteBuf.
     *
     * @return the body of the MessagePacket
     */
    public ByteBuf body() {
        return body;
    }

    /**
     * Returns the command identifier of this {@link MessagePacket}.
     *
     * @return the command identifier as an integer
     */
    public int command() {
        return command;
    }

    /**
     * Releases the resources held by this MessagePacket instance.
     * <p>
     * If the body is non-null, it will be released and set to null.
     * The instance itself will then be returned to the recycler for reuse.
     */
    @Override
    protected void deallocate() {
        if (null != body) {
            body.release();
            body = null;
        }
        handle.recycle(this);
    }

    /**
     * Retains a reference to this MessagePacket, incrementing its reference count by 1.
     *
     * @return this MessagePacket instance with an incremented reference count.
     */
    @Override
    public MessagePacket retain() {
        super.retain();
        return this;
    }

    /**
     * Retains the current MessagePacket by the specified increment.
     *
     * @param increment the additional number of references to retain
     * @return the current instance with retained references
     */
    @Override
    public MessagePacket retain(int increment) {
        super.retain(increment);
        return this;
    }

    /**
     * Increases the reference count of this object.
     * This method is called to indicate that the object is being accessed.
     *
     * @return This {@code MessagePacket} instance.
     */
    @Override
    public ReferenceCounted touch() {
        super.touch();
        return this;
    }

    /**
     * Updates the reference count of the message body with the provided hint and returns the current MessagePacket instance.
     *
     * @param hint the hint object to be passed to the touch method of the message body
     * @return the current instance of MessagePacket
     */
    @Override
    public MessagePacket touch(Object hint) {
        if (null != body) {
            body.touch(hint);
        }
        return this;
    }

    /**
     * Returns a string representation of the MessagePacket object.
     *
     * @return a string representation of the object, including feedback, command, and body fields.
     */
    @Override
    public String toString() {
        return "MessagePacket (handle=%s, feedback=%d, command=%d, body=%s)".formatted(handle, feedback, command, body);
    }

    /**
     * Converts the current MessagePacket to a Netty ByteBuf.
     *
     * @return a ByteBuf representation of the current MessagePacket, or null if an exception occurs
     */
    @Override
    public ByteBuf asByteBuf() {
        ByteBuf byteBuf = Unpooled.directBuffer();
        try {
            byteBuf.writeLong(feedback);
            byteBuf.writeInt(command);
            byteBuf.writeBytes(body);
            return byteBuf;
        } catch (Exception e) {
            if (byteBuf != null) {
                byteBuf.release();
            }
        }
        return null;
    }

    /**
     * Compares this MessagePacket with the specified MessagePacket for order.
     * Returns a negative integer, zero, or a positive integer as this MessagePacket
     * is less than, equal to, or greater than the specified MessagePacket.
     *
     * @param o the MessagePacket to be compared
     * @return a negative integer, zero, or a positive integer as this MessagePacket
     *         is less than, equal to, or greater than the specified MessagePacket
     */
    @Override
    public int compareTo(MessagePacket o) {
        assert o != null;
        int orb = o.body.readableBytes();
        return Integer.compare(this.body.readableBytes() - orb, 0);
    }
}
