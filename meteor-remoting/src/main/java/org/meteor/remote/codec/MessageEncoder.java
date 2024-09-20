package org.meteor.remote.codec;

import static org.meteor.remote.util.ByteBufUtil.release;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import io.netty.util.concurrent.PromiseCombiner;
import org.meteor.common.logging.InternalLogger;
import org.meteor.common.logging.InternalLoggerFactory;
import org.meteor.remote.exception.RemotingEncoderException;

/**
 * MessageEncoder is a specialized ChannelOutboundHandlerAdapter used
 * to encode MessagePacket objects into a ByteBuf. It is responsible
 * for constructing a properly formatted header and combining it with
 * the message body before writing to the Channel.
 * <p>
 * This class is implemented as a singleton and is thread-safe, which
 * allows the same instance to be shared across multiple channels.
 */
@ChannelHandler.Sharable
public final class MessageEncoder extends ChannelOutboundHandlerAdapter {
    private static final InternalLogger logger = InternalLoggerFactory.getLogger(MessageEncoder.class);
    /**
     * A static instance of the {@link MessageEncoder} class, used to handle the encoding of messages.
     * The encoder is responsible for writing message headers and bodies to the channel context.
     */
    private static final MessageEncoder ENCODER = new MessageEncoder();

    /**
     * Private constructor to prevent instantiation of the MessageEncoder class.
     *
     * This constructor is intentionally defined to throw an UnsupportedOperationException
     * to indicate that instantiation of this class is not supported. The class is designed
     * to be used statically through the other provided methods.
     *
     * @throws UnsupportedOperationException always thrown to indicate that creating an
     * instance of this class is not supported.
     */
    private MessageEncoder() {
        throw new UnsupportedOperationException("Cannot support the operation");
    }

    /**
     * Returns the singleton instance of the MessageEncoder.
     *
     * @return the singleton instance of MessageEncoder
     */
    public static MessageEncoder instance() {
        return ENCODER;
    }

    /**
     * Encodes and writes a {@link MessagePacket} to the given {@link ChannelHandlerContext}.
     * If the message is not an instance of {@link MessagePacket}, it delegates the write operation to the context.
     *
     * @param ctx the {@link ChannelHandlerContext} to which the packet should be written
     * @param msg the message to be written, expected to be an instance of {@link MessagePacket}
     * @param promise the {@link ChannelPromise} to be notified once the operation completes
     * @throws Exception if an error occurs during the encoding or writing process
     */
    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
        if (msg instanceof final MessagePacket packet) {
            final long feedback = packet.feedback();
            final int command = packet.command();
            final ByteBuf body = packet.body().retain();

            final ByteBuf header;
            try {
                header = encodeHeader(ctx.alloc(), command, feedback, body.readableBytes());
            } catch (Throwable cause) {
                release(body);
                if (logger.isDebugEnabled()) {
                    logger.debug(cause.getMessage(), cause);
                }
                throw cause;
            } finally {
                packet.release();
            }

            writeBuf(ctx, promise, header, body);
        } else {
            ctx.write(msg, promise);
        }
    }

    /**
     * Encodes the header for a message packet.
     *
     * @param alloc the ByteBufAllocator used to allocate the buffer
     * @param command the command identifier for the message
     * @param feedback the feedback identifier for the message
     * @param contentLength the length of the content in the message
     * @return the encoded header as a ByteBuf
     * @throws RemotingEncoderException if the contentLength exceeds the maximum allowed
     */
    private ByteBuf encodeHeader(ByteBufAllocator alloc, int command, long feedback, int contentLength) {
        if (contentLength > MessagePacket.MAX_BODY_LENGTH) {
            throw new RemotingEncoderException(
                    STR."The message body[\{contentLength}] bytes too long, limit[\{MessagePacket.MAX_BODY_LENGTH}] bytes");
        }

        final ByteBuf header = alloc.ioBuffer(MessagePacket.HEADER_LENGTH);
        header.writeByte(MessagePacket.MAGIC_NUMBER);
        header.writeMedium(contentLength + MessagePacket.HEADER_LENGTH);
        header.writeInt(command);
        header.writeLong(feedback);
        return header;
    }

    /**
     * Writes the provided header and body {@link ByteBuf} to the {@link ChannelHandlerContext}.
     *
     * If the body is not readable, it will be released and only the header will be written.
     * If the promise is void, both the header and body will be written immediately.
     * Otherwise, a {@link PromiseCombiner} is used to ensure that both write operations
     * complete before signaling the provided {@link ChannelPromise}.
     *
     * @param ctx the {@link ChannelHandlerContext} used to write the buffers
     * @param promise the {@link ChannelPromise} to be notified when the write operations complete
     * @param header the header {@link ByteBuf} to write
     * @param body the body {@link ByteBuf} to write
     */
    private void writeBuf(ChannelHandlerContext ctx, ChannelPromise promise, ByteBuf header, ByteBuf body) {
        if (!body.isReadable()) {
            body.release();
            ctx.write(header, promise);
            return;
        }

        if (promise.isVoid()) {
            ctx.write(header, promise);
            ctx.write(body, promise);
            return;
        }
        final PromiseCombiner combiner = new PromiseCombiner(ctx.executor());
        combiner.add(ctx.write(header));
        combiner.add(ctx.write(body));
        combiner.finish(promise);
    }
}
