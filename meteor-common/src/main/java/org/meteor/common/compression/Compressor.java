package org.meteor.common.compression;

import java.io.IOException;

/**
 * The Compressor interface defines the operations for compressing and decompressing
 * byte arrays using a specific compression algorithm and level.
 */
public interface Compressor {
    /**
     * Compresses the given byte array using the specified compression level.
     *
     * @param src   the byte array to be compressed
     * @param level the compression level to be applied, where higher values typically indicate higher compression
     * @return the compressed byte array
     * @throws IOException if an I/O error occurs during compression
     */
    byte[] compress(byte[] src, int level) throws IOException;

    /**
     * Decompresses the given byte array using a specific compression algorithm and level.
     *
     * @param src the byte array to be decompressed
     * @param level the compression level to be used for decompression
     * @return the decompressed byte array
     * @throws IOException if an I/O error occurs during decompression
     */
    byte[] decompress(byte[] src, int level) throws IOException;
}
