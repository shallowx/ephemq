package org.shallow.codec;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.CompositeByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.DecoderException;

import static java.lang.Integer.MAX_VALUE;
import static org.shallow.util.ObjectUtil.isNotNull;
import static org.shallow.util.ObjectUtil.isNull;
import static org.shallow.codec.MessageDecoder.State.*;
import static org.shallow.util.ByteBufUtil.release;

public final class MessageDecoder extends ChannelInboundHandlerAdapter {

    private static final int DISCARD_READ_BODY_THRESHOLD = 3;
    enum State {
        READ_MAGIC_NUMBER, READ_MESSAGE_LENGTH, READ_MESSAGE_COMPLETED
    }

    private ByteBuf whole;
    private boolean invalid;
    private State state = READ_MAGIC_NUMBER;
    private int writeFrameBytes;

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        if (msg instanceof final ByteBuf read) {
            if (invalid) {
                read.release();
                return;
            }

            ByteBuf buf = null;
            try {
                buf = whole(ctx.alloc(), read).retain();
                while (!ctx.isRemoved() && buf.isReadable()) {
                    final MessagePacket packet = decode(buf);
                    if (isNull(packet)) {
                        break;
                    }
                    ctx.fireChannelRead(packet);
                }
            } catch (Throwable cause) {
                invalid = true;
                throw cause;
            } finally {
              release(buf);

              buf = whole;
              if (isNotNull(buf) && (!buf.isReadable() || invalid)) {
                  whole = null;
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

                final int frame = buf.getUnsignedMedium(buf.readerIndex() + 1);
                if (frame < MessagePacket.HEADER_LENGTH) {
                    throw new DecoderException("Invalid frame length:" + frame);
                }

                writeFrameBytes = frame;
                state = READ_MESSAGE_COMPLETED;
            }
            case READ_MESSAGE_COMPLETED:{
                if (!buf.isReadable(writeFrameBytes)) {
                    return null;
                }
                buf.skipBytes(4);

                final short version = buf.readShort();
                final byte command = buf.readByte();
                final byte type = buf.readByte();
                final int answer = buf.readInt();
                final ByteBuf body = buf.readRetainedSlice(writeFrameBytes - MessagePacket.HEADER_LENGTH);

                this.state = READ_MAGIC_NUMBER;

                return MessagePacket.newPacket(version, type, answer, command, body);
            }
            default:{
                throw new DecoderException("Invalid decode state:" + state);
            }
        }
    }

    private ByteBuf whole(ByteBufAllocator alloc, ByteBuf read) {
        final ByteBuf buf = whole;
        if (isNull(buf)) {
            return whole = read;
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

        composite.addFlattenedComponents(true, read);
        return whole = composite;
    }

    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
        if (whole instanceof final CompositeByteBuf buf) {
            if (buf.toComponentIndex(buf.readerIndex()) > DISCARD_READ_BODY_THRESHOLD) {
                whole = buf.refCnt() == 1 ? buf.discardReadComponents() : newComposite(ctx.alloc(), buf);
            }
        }

        ctx.fireChannelReadComplete();
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        if (isNotNull(whole)) {
            release(whole);
            whole = null;
        }

        ctx.fireChannelInactive();
    }

    @Override
    public void handlerRemoved(ChannelHandlerContext ctx) throws Exception {
        final ByteBuf buf = whole;
        if (isNotNull(buf)) {
            whole = null;
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
