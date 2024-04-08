package org.meteor.common.compression;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import net.jpountz.lz4.LZ4FrameInputStream;
import net.jpountz.lz4.LZ4FrameOutputStream;

public class Lz4Compressor implements Compressor {
    @Override
    public byte[] compress(byte[] src, int level) throws IOException {
        byte[] result = src;
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream(src.length);
        try (byteArrayOutputStream) {
            LZ4FrameOutputStream outputStream = new LZ4FrameOutputStream(byteArrayOutputStream);
            outputStream.write(src);
            outputStream.flush();
            outputStream.close();
            result = byteArrayOutputStream.toByteArray();
        }
        return result;
    }

    @Override
    public byte[] decompress(byte[] src, int level) throws IOException {
        byte[] result = src;
        byte[] data = new byte[src.length];
        ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(src);
        ByteArrayOutputStream resultOutputStream = new ByteArrayOutputStream(src.length);

        try (byteArrayInputStream; LZ4FrameInputStream lz4InputStream = new LZ4FrameInputStream(byteArrayInputStream)) {
            while (true) {
                int len = lz4InputStream.read(data, 0, data.length);
                if (len <= 0) {
                    break;
                }
                resultOutputStream.write(data, 0, len);
            }
            resultOutputStream.flush();
            resultOutputStream.close();
            result = resultOutputStream.toByteArray();
        }
        return result;
    }
}
