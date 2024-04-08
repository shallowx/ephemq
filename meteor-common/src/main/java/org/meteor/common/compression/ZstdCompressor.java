package org.meteor.common.compression;

import com.github.luben.zstd.ZstdInputStream;
import com.github.luben.zstd.ZstdOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class ZstdCompressor implements Compressor {

    @Override
    public byte[] compress(byte[] src, int level) throws IOException {
        byte[] result = src;
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream(src.length);
        try (byteArrayOutputStream) {
            ZstdOutputStream outputStream = new ZstdOutputStream(byteArrayOutputStream, level);
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
        try (byteArrayInputStream; ZstdInputStream zstdInputStream = new ZstdInputStream(byteArrayInputStream)) {
            while (true) {
                int len = zstdInputStream.read(data, 0, data.length);
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
