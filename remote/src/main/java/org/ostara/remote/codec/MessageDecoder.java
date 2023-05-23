package org.ostara.remote.codec;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.CompositeByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.DecoderException;

import static java.lang.Integer.MAX_VALUE;
import static org.ostara.remote.codec.MessageDecoder.State.*;
import static org.ostara.remote.util.ByteBufUtils.release;

public final class MessageDecoder extends ChannelInboundHandlerAdapter {

    private static final int DISCARD_READ_BODY_THRESHOLD = 3;
    enum State {
        READ_MAGIC_NUMBER, READ_MESSAGE_LENGTH, READ_MESSAGE_COMPLETED
    }

    private ByteBuf accumulation;
    private boolean invalidChannel;
    private State state = READ_MAGIC_NUMBER;
    private int writeFrameBytes;

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        if (msg instanceof final ByteBuf in) {
            if (invalidChannel) {
                in.release();
                return;
            }

            ByteBuf buf = null;
            try {
                buf = accumulation(ctx.alloc(), in).retain();
                while (!ctx.isRemoved() && buf.isReadable()) {
                    final MessagePacket packet = decode(buf);
                    if (null == packet) {
                        break;
                    }
                    ctx.fireChannelRead(packet);
                }
            } catch (Throwable cause) {
                invalidChannel = true;
                throw cause;
            } finally {
              release(buf);

              buf = accumulation;
              if (null != buf && (!buf.isReadable() || invalidChannel)) {
                  accumulation = null;
                  release(buf);
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
                    throw new DecoderException("Invalid magic number:" + magic);
                }

                state = READ_MESSAGE_LENGTH;
            }
            case READ_MESSAGE_LENGTH: {
                if (!buf.isReadable(4)) {
                    return null;
                }

                final int frameLength = buf.getUnsignedMedium(buf.readerIndex() + 1);
                if (frameLength < MessagePacket.HEADER_LENGTH) {
                    throw new DecoderException("Invalid frame length:" + frameLength);
                }

                writeFrameBytes = frameLength;
                state = READ_MESSAGE_COMPLETED;
            }
            case READ_MESSAGE_COMPLETED:{
                if (!buf.isReadable(writeFrameBytes)) {
                    return null;
                }
                buf.skipBytes(4);

                final int command = buf.readInt();
                final int answer = buf.readInt();
                final ByteBuf body = buf.readRetainedSlice(writeFrameBytes - MessagePacket.HEADER_LENGTH);

                this.state = READ_MAGIC_NUMBER;

                return MessagePacket.newPacket(answer, command, body);
            }
            default:{
                throw new DecoderException("Invalid decode state:" + state);
            }
        }
    }

    private ByteBuf accumulation(ByteBufAllocator alloc, ByteBuf in) {
        final ByteBuf buf = accumulation;
        if (null == buf) {
            return accumulation = in;
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
        return accumulation = composite;
    }

    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
        if (accumulation instanceof final CompositeByteBuf buf) {
            if (buf.toComponentIndex(buf.readerIndex()) > DISCARD_READ_BODY_THRESHOLD) {
                accumulation = buf.refCnt() == 1 ? buf.discardReadComponents() : newComposite(ctx.alloc(), buf);
            }
        }

        ctx.fireChannelReadComplete();
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        if (null != accumulation) {
            release(accumulation);
            accumulation = null;
        }

        ctx.fireChannelInactive();
    }

    @Override
    public void handlerRemoved(ChannelHandlerContext ctx) throws Exception {
        final ByteBuf buf = accumulation;
        if (null != buf) {
            accumulation = null;
            if (buf.isReadable()) {
                ctx.fireChannelRead(buf);
                ctx.fireChannelReadComplete();
            } else {
                release(buf);
            }
        }
    }

    private static CompositeByteBuf newComposite(ByteBufAllocator alloc, ByteBuf buf) {
        return alloc.compositeBuffer(MAX_VALUE).addFlattenedComponents(true, buf);
    }
}
