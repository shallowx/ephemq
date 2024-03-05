package org.meteor.remote.codec;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.CompositeByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.DecoderException;
import org.meteor.common.logging.InternalLogger;
import org.meteor.common.logging.InternalLoggerFactory;
import org.meteor.remote.util.ByteBufUtil;

import static java.lang.Integer.MAX_VALUE;
import static org.meteor.remote.codec.MessageDecoder.State.*;

public final class MessageDecoder extends ChannelInboundHandlerAdapter {
    private static final InternalLogger logger = InternalLoggerFactory.getLogger(MessageDecoder.class);
    private static final int DISCARD_READ_BODY_THRESHOLD = 3;
    private State state = READ_MAGIC_NUMBER;
    private ByteBuf composite;
    private boolean isValid;
    private int writeFrameBytes;

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

    private MessagePacket decode(ByteBuf buf) {
        switch (state) {
            case READ_MAGIC_NUMBER: {
                if (!buf.isReadable()) {
                    return null;
                }

                final byte magic = buf.getByte(buf.readerIndex());
                if (magic != MessagePacket.MAGIC_NUMBER) {
                    throw new DecoderException("Invalid magic number[" + magic + "]");
                }

                state = READ_MESSAGE_LENGTH;
            }
            case READ_MESSAGE_LENGTH: {
                if (!buf.isReadable(4)) {
                    return null;
                }

                final int messageLength = buf.getUnsignedMedium(buf.readerIndex() + 1);
                if (messageLength < MessagePacket.HEADER_LENGTH) {
                    throw new DecoderException("Invalid message length[" + messageLength + "]");
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
                throw new DecoderException("Invalid decode state[" + state +"]");
            }
        }
    }

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

    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
        if (composite instanceof final CompositeByteBuf buf) {
            if (buf.toComponentIndex(buf.readerIndex()) > DISCARD_READ_BODY_THRESHOLD) {
                composite = buf.refCnt() == 1 ? buf.discardReadComponents() : newComposite(ctx.alloc(), buf);
            }
        }

        ctx.fireChannelReadComplete();
    }

    private static CompositeByteBuf newComposite(ByteBufAllocator alloc, ByteBuf buf) {
        return alloc.compositeBuffer(MAX_VALUE).addFlattenedComponents(true, buf);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        if (null != composite) {
            composite.release();
            composite = null;
        }

        ctx.fireChannelInactive();
    }

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
