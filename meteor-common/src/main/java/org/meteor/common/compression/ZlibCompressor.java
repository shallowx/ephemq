package org.meteor.common.compression;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.InflaterInputStream;

/**
 * The ZlibCompressor class implements the Compressor interface, providing
 * methods to compress and decompress byte arrays using the ZLIB compression algorithm.
 */
public class ZlibCompressor implements Compressor {
    /**
     * Compresses the given byte array using the specified compression level.
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
        java.util.zip.Deflater defeater = new java.util.zip.Deflater(level);
        DeflaterOutputStream deflaterOutputStream = new DeflaterOutputStream(byteArrayOutputStream, defeater);
        try {
            deflaterOutputStream.write(src);
            deflaterOutputStream.finish();
            deflaterOutputStream.close();
            result = byteArrayOutputStream.toByteArray();
        } catch (IOException e) {
            defeater.end();
            throw e;
        } finally {
            try {
                byteArrayOutputStream.close();
            } catch (IOException ignored) {
            }
            defeater.end();
        }
        return result;
    }

    /**
     * Decompresses the given byte array using a specific compression algorithm and level.
     *
     * @param src the byte array to be decompressed
     * @param level the compression level to be used for decompression
     * @return the decompressed byte array
     * @throws IOException if an I/O error occurs during decompression
     */
    @Override
    public byte[] decompress(byte[] src, int level) throws IOException {
        byte[] result = src;
        byte[] data = new byte[src.length];
        ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(src);

        try (byteArrayInputStream;
             InflaterInputStream inflaterInputStream = new InflaterInputStream(byteArrayInputStream);
             ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream(src.length)) {
            while (true) {
                int len = inflaterInputStream.read(data, 0, data.length);
                if (len <= 0) {
                    break;
                }
                byteArrayOutputStream.write(data, 0, len);
            }
            byteArrayOutputStream.flush();
            result = byteArrayOutputStream.toByteArray();
        }
        return result;
    }
}
