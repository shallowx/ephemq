package org.meteor.common.compression;

public class CompressionConstants {
    public final static int COMPRESSION_NO_TYPE = ~(0x1 << 8);
    public final static int COMPRESSION_LZ4_TYPE = 0x1 << 8;
    public final static int COMPRESSION_ZSTD_TYPE = 0x2 << 8;
    public final static int COMPRESSION_ZLIB_TYPE = 0x3 << 8;
    public final static int COMPRESSION_TYPE_COMPARATOR = 0x7 << 8;
}
