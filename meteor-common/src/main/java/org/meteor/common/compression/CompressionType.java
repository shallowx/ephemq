package org.meteor.common.compression;

/**
 * Enumeration representing different types of compression algorithms.
 * Each enum constant holds a specific integer value, which can be retrieved
 * or converted using provided methods.
 */
public enum CompressionType {
    /**
     * Represents no compression type within the CompressionType enum.
     * This enum constant is associated with a value of -1 and is used to indicate
     * the absence of any compression algorithm.
     */
    No(-1),
    /**
     * Represents the LZ4 compression algorithm.
     * This compression type is used for fast data compression and decompression,
     * leveraging the LZ4 algorithm known for its high-speed capabilities.
     * It holds an integer value of 1.
     */
    LZ4(1),
    /**
     * Represents the constant for Zstandard (ZSTD) compression type.
     * This constant is part of the enumeration of different compression algorithms.
     * The associated integer value is used to identify this specific compression type.
     */
    ZSTD(2),
    /**
     * Represents the ZLIB compression type.
     * This compression type uses the ZLIB algorithm and is associated with the integer value 3.
     */
    ZLIB(3);

    /**
     * Holds the integer value representing the specific type of compression algorithm.
     * This value is used to uniquely identify and retrieve the corresponding
     * compression type.
     */
    private final int value;

    /**
     * Constructs a CompressionType with the specified integer value.
     *
     * @param value the integer value representing the compression type
     */
    CompressionType(int value) {
        this.value = value;
    }

    /**
     * Retrieves the integer value associated with this compression type.
     *
     * @return the integer value representing the compression type.
     */
    public int getValue() {
        return value;
    }

    /**
     * Converts a string representation of a compression type to its corresponding
     * CompressionType enum constant. The input string is trimmed and converted
     * to uppercase to match the predefined compression type names.
     *
     * @param type the string representation of the compression type
     * @return the corresponding CompressionType enum constant
     * @throws RuntimeException if the specified type does not match any supported compression types
     */
    public static CompressionType of(String type) {
        return switch (type.trim().toUpperCase()) {
            case "NO" -> CompressionType.No;
            case "LZ4" -> CompressionType.LZ4;
            case "ZSTD" -> CompressionType.ZSTD;
            case "ZLIB" -> CompressionType.ZLIB;
            default -> throw new RuntimeException("Unsupported compress type name: " + type);
        };
    }

    /**
     * Returns the CompressionType corresponding to the given integer value.
     *
     * @param value the integer value representing a specific compression type
     * @return the CompressionType associated with the provided integer value
     * @throws RuntimeException if the provided value does not correspond to any known compression type
     */
    public static CompressionType getType(int value) {
        return switch (value) {
            case -1 -> No;
            case 1 -> LZ4;
            case 2 -> ZSTD; // To be compatible for older versions without compression type
            case 0, 3 -> ZLIB;
            default -> throw new RuntimeException("Unknown compress type value: " + value);
        };
    }

    /**
     * Retrieves the compression flag corresponding to the compression type.
     * The returned flag is an integer value defined in CompressionConstants,
     * representing the specific compression algorithm.
     *
     * @return the integer compression flag corresponding to the compression type
     * @throws RuntimeException if the compression type is unsupported
     */
    public int getCompressionFlag() {
        return switch (value) {
            case -1 -> CompressionConstants.COMPRESSION_NO_TYPE;
            case 1 -> CompressionConstants.COMPRESSION_LZ4_TYPE;
            case 2 -> CompressionConstants.COMPRESSION_ZSTD_TYPE;
            case 3 -> CompressionConstants.COMPRESSION_ZLIB_TYPE;
            default -> throw new RuntimeException("Unsupported compress type flag: " + value);
        };
    }
}
