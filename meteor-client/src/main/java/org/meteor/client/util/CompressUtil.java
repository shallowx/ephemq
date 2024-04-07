package org.meteor.client.util;

import io.netty.buffer.ByteBuf;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.zip.Deflater;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.InflaterInputStream;
import org.meteor.remote.util.ByteBufUtil;

public class CompressUtil {

    public static ByteBuf compress(final ByteBuf buf, final int level, final boolean needCompress) throws IOException {
        if (!needCompress) {
            return buf;
        }

        byte[] src = ByteBufUtil.buf2Bytes(buf);
        byte[] result;
        ByteArrayOutputStream out = new ByteArrayOutputStream(src.length);
        Deflater deflater = new Deflater(level);
        DeflaterOutputStream deflaterOutputStream = new DeflaterOutputStream(out, deflater);
        try {
            deflaterOutputStream.write(src);
            deflaterOutputStream.finish();
            deflaterOutputStream.close();
            result = out.toByteArray();
        } catch (IOException e) {
            deflater.end();
            throw e;
        } finally {
            try {
                out.close();
            } catch (IOException ignored) {
            }
        }
        return ByteBufUtil.byte2Buf(result);
    }

    public static ByteBuf uncompress(final ByteBuf buf, final int level, final boolean needCompress)
            throws IOException {
        if (!needCompress) {
            return buf;
        }
        byte[] src = ByteBufUtil.buf2Bytes(buf);
        byte[] result = new byte[src.length];
        ByteArrayInputStream in = new ByteArrayInputStream(src);
        try (in; InflaterInputStream inflaterInputStream = new InflaterInputStream(in);
             ByteArrayOutputStream out = new ByteArrayOutputStream(src.length)) {
            while (true) {
                int len = inflaterInputStream.read(result);
                if (len == -1) {
                    break;
                }
                out.write(result, 0, len);
            }
            out.flush();
            result = out.toByteArray();
        }
        return ByteBufUtil.byte2Buf(result);
    }

    public boolean needCompress(ByteBuf buf) {
        int length = ByteBufUtil.bufLength(buf);
        return length >= MessageConstants.MESSAGE_MAX_LENGTH;
    }
}
