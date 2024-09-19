package org.meteor.common.compression;

import java.util.EnumMap;

/**
 * Factory class to obtain instances of {@link Compressor} based on the {@link CompressionType}.
 * This class initializes a map of compression types to their corresponding compressor instances.
 */
public class CompressorFactory {
    /**
     * A static final map that holds the association between {@link CompressionType} enum constants
     * and their corresponding {@link Compressor} instances. This map is used to retrieve a compressor
     * based on the specified compression type.
     */
    private static final EnumMap<CompressionType, Compressor> COMPRESSORS;
    static {
        COMPRESSORS = new EnumMap<>(CompressionType.class);
        COMPRESSORS.put(CompressionType.LZ4, new Lz4Compressor());
        COMPRESSORS.put(CompressionType.ZSTD, new ZstdCompressor());
        COMPRESSORS.put(CompressionType.ZLIB, new ZlibCompressor());
    }

    /**
     * Retrieves a compressor instance based on the provided compression type.
     *
     * @param type the type of compression algorithm to retrieve the compressor for
     * @return the compressor instance corresponding to the specified compression type
     */
    public static Compressor getCompressor(CompressionType type) {
        return COMPRESSORS.get(type);
    }
}
