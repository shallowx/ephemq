package org.meteor.remote.codec;

import static java.lang.Integer.MAX_VALUE;
import static org.meteor.remote.codec.MessageDecoder.State.READ_MAGIC_NUMBER;
import static org.meteor.remote.codec.MessageDecoder.State.READ_MESSAGE_COMPLETED;
import static org.meteor.remote.codec.MessageDecoder.State.READ_MESSAGE_LENGTH;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.CompositeByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import org.meteor.common.logging.InternalLogger;
import org.meteor.common.logging.InternalLoggerFactory;
import org.meteor.remote.exception.RemotingDecoderException;
import org.meteor.remote.util.ByteBufUtil;

/**
 * The {@code MessageDecoder} class is responsible for decoding inbound messages
 * from a {@link ByteBuf} and converting them into {@link MessagePacket} objects.
 * This class extends {@code ChannelInboundHandlerAdapter} to handle the read,
 * read completion, handler removal, and channel inactive events in a Netty
 * pipeline.
 */
public final class MessageDecoder extends ChannelInboundHandlerAdapter {
    private static final InternalLogger logger = InternalLoggerFactory.getLogger(MessageDecoder.class);
    /**
     * The threshold for the number of times a read body operation is allowed before the body is discarded.
     * This is a static constant used by the {@code MessageDecoder} class to manage read operations.
     * Once this threshold is reached, the message body will be discarded to prevent further processing.
     */
    private static final int DISCARD_READ_BODY_THRESHOLD = 3;
    /**
     * Represents the current state of the message decoding process
     * in the MessageDecoder class.
     * <p>
     * Possible states are:
     * - READ_MAGIC_NUMBER: Waiting to read the magic number from the message header.
     * - READ_MESSAGE_LENGTH: Waiting to read the length of the message body.
     * - READ_MESSAGE_COMPLETED: The message body has been completely read.
     */
    private State state = READ_MAGIC_NUMBER;
    /**
     * A buffer that holds parts of a message being decoded.
     *
     * This buffer is used to accumulate data until there's enough to decode a complete message.
     */
    private ByteBuf composite;
    /**
     * Indicates the validity of the current state or operation in the MessageDecoder.
     */
    private boolean isValid;
    /**
     * Holds the count of bytes that have been written for the current message frame.
     * Used in the {@link MessageDecoder} class to manage and track the amount of data being processed.
     */
    private int writeFrameBytes;

    /**
     * Creates a new CompositeByteBuf by adding the given ByteBuf component.
     *
     * @param alloc The ByteBufAllocator used to allocate the new CompositeByteBuf.
     * @param buf The ByteBuf to be added as a component to the CompositeByteBuf.
     * @return A newly created CompositeByteBuf with the given ByteBuf as its component.
     */
    private static CompositeByteBuf newComposite(ByteBufAllocator alloc, ByteBuf buf) {
        return alloc.compositeBuffer(MAX_VALUE).addFlattenedComponents(true, buf);
    }

    /**
     * Handles the read operation from the channel by processing the incoming message.
     *
     * @param ctx the {@link ChannelHandlerContext} which provides various operations that enable
     *            you to trigger various I/O events and operations.
     * @param msg the message to be read. If it's a {@link ByteBuf}, the method attempts to decode it
     *            into {@link MessagePacket} instances and fire further channel read events.
     * @throws Exception if an error occurs while processing the message, it throws the exception
     *                   to indicate failure in processing.
     */
    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        if (msg instanceof final ByteBuf in) {
            if (isValid) {
                in.release();
                if (logger.isDebugEnabled()) {
                    logger.debug("Invalid channel[{}]", ctx.channel().toString());
                }
                return;
            }

            ByteBuf buf = null;
            try {
                buf = allocComposite(ctx.alloc(), in).retain();
                while (!ctx.isRemoved() && buf.isReadable()) {
                    final MessagePacket packet = decode(buf);
                    if (null == packet) {
                        break;
                    }
                    ctx.fireChannelRead(packet);
                }
            } catch (Throwable cause) {
                isValid = true;
                if (logger.isDebugEnabled()) {
                    logger.debug(cause.getMessage(), cause);
                }
                throw cause;
            } finally {
                ByteBufUtil.release(buf);
                buf = composite;
                if (null != buf && (!buf.isReadable() || isValid)) {
                    composite = null;
                    buf.release();
                }
            }
        } else {
            ctx.fireChannelRead(msg);
        }
    }

    /**
     * Decodes a {@link ByteBuf} into a {@link MessagePacket} based on predefined message structure.
     *
     * @param buf the buffer containing the message to be decoded
     * @return a decoded {@link MessagePacket}, or null if the buffer does not have enough data
     * @throws RemotingDecoderException if an error occurs during decoding, such as an invalid magic number or message length
     */
    private MessagePacket decode(ByteBuf buf) {
        switch (state) {
            case READ_MAGIC_NUMBER: {
                if (!buf.isReadable()) {
                    return null;
                }

                final byte magic = buf.getByte(buf.readerIndex());
                if (magic != MessagePacket.MAGIC_NUMBER) {
                    throw new RemotingDecoderException(STR."Invalid magic number[\{magic}]");
                }

                state = READ_MESSAGE_LENGTH;
            }
            case READ_MESSAGE_LENGTH: {
                if (!buf.isReadable(4)) {
                    return null;
                }

                final int messageLength = buf.getUnsignedMedium(buf.readerIndex() + 1);
                if (messageLength < MessagePacket.HEADER_LENGTH) {
                    throw new RemotingDecoderException(STR."Invalid message length[\{messageLength}]");
                }

                writeFrameBytes = messageLength;
                state = READ_MESSAGE_COMPLETED;
            }
            case READ_MESSAGE_COMPLETED: {
                if (!buf.isReadable(writeFrameBytes)) {
                    return null;
                }
                buf.skipBytes(4);

                final int command = buf.readInt();
                final long feedback = buf.readLong();
                final ByteBuf body = buf.readRetainedSlice(writeFrameBytes - MessagePacket.HEADER_LENGTH);
                this.state = READ_MAGIC_NUMBER;
                return MessagePacket.newPacket(feedback, command, body);
            }
            default: {
                throw new RemotingDecoderException(STR."Invalid decode state[\{state}]");
            }
        }
    }

    /**
     * Allocates a composite {@link ByteBuf} by either returning the existing composite buffer
     * or creating a new composite buffer that includes the provided {@link ByteBuf}.
     *
     * @param alloc the {@link ByteBufAllocator} to allocate new buffers if needed
     * @param in the input {@link ByteBuf} to be added to the composite buffer
     * @return the allocated composite {@link ByteBuf}
     */
    private ByteBuf allocComposite(ByteBufAllocator alloc, ByteBuf in) {
        final ByteBuf buf = composite;
        if (null == buf) {
            return composite = in;
        }

        final CompositeByteBuf composite;
        if (buf instanceof CompositeByteBuf && buf.refCnt() == 1) {
            composite = (CompositeByteBuf) buf;
            if (composite.writerIndex() != composite.capacity()) {
                composite.capacity(composite.writerIndex());
            }
        } else {
            composite = newComposite(alloc, buf);
        }

        composite.addFlattenedComponents(true, in);
        return this.composite = composite;
    }

    /**
     * Handles the completion of a read operation on a channel.
     *
     * This method is invoked when the current read operation on the channel has completed. It checks
     * whether the `composite` instance is of type `CompositeByteBuf`. If the buffer's component index
     * at the current reader index exceeds the `DISCARD_READ_BODY_THRESHOLD`, and if the buffer has only
     * one reference, it discards read components to optimize memory usage. If the buffer has more than
     * one reference, it replaces the `composite` with a new composite buffer.
     *
     * @param ctx the context of the channel handler providing channel pipeline operations and state.
     * @throws Exception if any error occurs during processing.
     */
    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
        if (composite instanceof final CompositeByteBuf buf) {
            if (buf.toComponentIndex(buf.readerIndex()) > DISCARD_READ_BODY_THRESHOLD) {
                composite = buf.refCnt() == 1 ? buf.discardReadComponents() : newComposite(ctx.alloc(), buf);
            }
        }

        ctx.fireChannelReadComplete();
    }

    /**
     * This method is invoked when the channel becomes inactive. It releases the composite
     * buffer if it is not null and sets it to null, then proceeds to invoke the next handler
     * in the pipeline using `ctx.fireChannelInactive()`.
     *
     * @param ctx the {@link ChannelHandlerContext} which this {@link ChannelInboundHandlerAdapter} belongs to
     * @throws Exception thrown if an error occurs during the process
     */
    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        if (null != composite) {
            composite.release();
            composite = null;
        }

        ctx.fireChannelInactive();
    }

    /**
     * Handles the removal of the handler from the {@link ChannelHandlerContext}. This method is called
     * when the handler is removed from the pipeline. If the composite buffer is not null and readable,
     * it fires a channel read event and a channel read complete event; otherwise, it releases the buffer.
     *
     * @param ctx the context object for this handler, which provides various operations that
     *        enable you to trigger state changes and interact with the pipeline.
     */
    @Override
    public void handlerRemoved(ChannelHandlerContext ctx) throws Exception {
        final ByteBuf buf = composite;
        if (null != buf) {
            composite = null;
            if (buf.isReadable()) {
                ctx.fireChannelRead(buf);
                ctx.fireChannelReadComplete();
            } else {
                buf.release();
            }
        }
    }

    enum State {
        READ_MAGIC_NUMBER, READ_MESSAGE_LENGTH, READ_MESSAGE_COMPLETED
    }
}
