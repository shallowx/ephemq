package org.meteor.common.compression;

import com.github.luben.zstd.ZstdInputStream;
import com.github.luben.zstd.ZstdOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

/**
 * Implements the Compressor interface using the Zstandard (ZSTD) compression algorithm.
 * Provides methods to compress and decompress byte arrays with a specified compression level.
 */
public class ZstdCompressor implements Compressor {

    /**
     * Compresses the given byte array using the specified compression level with the Zstandard (ZSTD) algorithm.
     *
     * @param src   the byte array to be compressed
     * @param level the compression level to be applied, where higher values typically indicate higher compression
     * @return the compressed byte array
     * @throws IOException if an I/O error occurs during compression
     */
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

    /**
     * Decompresses the given byte array using the Zstandard (ZSTD) compression algorithm.
     *
     * @param src the byte array to be decompressed.
     * @param level the compression level to be used for decompression.
     * @return the decompressed byte array.
     * @throws IOException if an I/O error occurs during decompression.
     */
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
