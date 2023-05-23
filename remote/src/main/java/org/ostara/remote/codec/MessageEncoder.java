package org.ostara.remote.codec;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.EncoderException;
import io.netty.util.concurrent.PromiseCombiner;

import static org.ostara.remote.util.ByteBufUtils.release;

@ChannelHandler.Sharable
public final class MessageEncoder extends ChannelOutboundHandlerAdapter {

    private static final MessageEncoder ENCODER = new MessageEncoder();

    private MessageEncoder() {}

    public static MessageEncoder instance() {
        return ENCODER;
    }

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
        if (msg instanceof final MessagePacket packet) {
            final int answer = packet.answer();
            final int command = packet.command();
            final ByteBuf body = packet.body().retain();

            final ByteBuf header;
            try {
                 header = encodeHeader(ctx.alloc(), command, answer, body.readableBytes());
            } catch (Throwable cause) {
                release(body);
                throw cause;
            } finally {
                packet.release();
            }

            writeBuf(ctx, promise, header, body);
        } else {
            ctx.write(msg, promise);
        }
    }

    private ByteBuf encodeHeader(ByteBufAllocator alloc, int command, int answer, int contentLength) {
        if (contentLength > MessagePacket.MAX_BODY_LENGTH) {
            throw new EncoderException("Too large body:" + contentLength + "bytes, limit:" + MessagePacket.MAX_BODY_LENGTH + "bytes");
        }

        final ByteBuf header = alloc.ioBuffer(MessagePacket.HEADER_LENGTH);
        header.writeByte(MessagePacket.MAGIC_NUMBER);
        header.writeMedium(contentLength + MessagePacket.HEADER_LENGTH);
        header.writeInt(command);
        header.writeInt(answer);

        return header;
    }

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
