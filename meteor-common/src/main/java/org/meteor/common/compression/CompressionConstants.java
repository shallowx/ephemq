package org.meteor.common.compression;

/**
 * The CompressionConstants class defines constants for various types of
 * compression methods used throughout the application. Each constant
 * represents a specific compression type encoded as an integer value.
 */
public class CompressionConstants {
    /**
     * Represents the absence of any specific compression type.
     * This constant is the bitwise NOT of 256 (0x1 shifted left by 8 bits) and
     * is used to indicate that no compression method is specified.
     */
    public final static int COMPRESSION_NO_TYPE = ~(0x1 << 8);
    /**
     * Represents the LZ4 compression type. This constant is used to
     * indicate that data compression or decompression should be
     * performed using the LZ4 algorithm. The value is bit-shifted
     * left by 8 positions, resulting in a unique identifier for this
     * compression type.
     */
    public final static int COMPRESSION_LZ4_TYPE = 0x1 << 8;
    /**
     * Represents the constant for Zstandard (ZSTD) compression type.
     * This constant is part of the compression type encoding system
     * and is used to specify that ZSTD compression should be applied
     * based on its integer value representation.
     */
    public final static int COMPRESSION_ZSTD_TYPE = 0x2 << 8;
    /**
     * Constant representing the ZLIB compression type.
     * This value is used to specify that the ZLIB compression algorithm
     * should be utilized. It is encoded as an integer value with a specific bit mask.
     */
    public final static int COMPRESSION_ZLIB_TYPE = 0x3 << 8;
    /**
     * A constant representing the mask used to extract or compare the compression type
     * from a composite integer value. The compression type is encoded within the integer
     * by shifting a base value left by 8 bits. This constant aids in isolating or
     * matching the compression type field.
     */
    public final static int COMPRESSION_TYPE_COMPARATOR = 0x7 << 8;
}
