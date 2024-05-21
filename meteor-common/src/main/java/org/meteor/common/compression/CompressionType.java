package org.meteor.common.compression;

public enum CompressionType {
    No(-1),
    LZ4(1),
    ZSTD(2),
    ZLIB(3);

    private final int value;

    CompressionType(int value) {
        this.value = value;
    }

    public int getValue() {
        return value;
    }

    public static CompressionType of(String type) {
        return switch (type.trim().toUpperCase()) {
            case "NO" -> CompressionType.No;
            case "LZ4" -> CompressionType.LZ4;
            case "ZSTD" -> CompressionType.ZSTD;
            case "ZLIB" -> CompressionType.ZLIB;
            default -> throw new RuntimeException("Unsupported compress type name: " + type);
        };
    }

    public static CompressionType getType(int value) {
        return switch (value) {
            case -1 -> No;
            case 1 -> LZ4;
            case 2 -> ZSTD; // To be compatible for older versions without compression type
            case 0, 3 -> ZLIB;
            default -> throw new RuntimeException("Unknown compress type value: " + value);
        };
    }

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
