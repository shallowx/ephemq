package org.ephemq.common.commpression;

import org.apache.commons.lang3.RandomStringUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.ephemq.common.compression.CompressionType;
import org.ephemq.common.compression.Compressor;
import org.ephemq.common.compression.CompressorFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Random;

public class CompressionTest {
    /**
     * Compression level to be used in various compression tests.
     * Higher values typically indicate a higher degree of compression.
     */
    private int level;
    /**
     * An instance of the Compressor interface specifically implementing the Zstandard (ZSTD)
     * compression algorithm. This instance is used for compressing and decompressing byte arrays
     * with the ZSTD algorithm.
     *
     * <p>The ZSTD compressor is known for providing high compression ratios while maintaining
     * reasonably fast compression and decompression speeds. It is suitable for scenarios where
     * storage efficiency is crucial.
     *
     * <p>This instance is configured and utilized within the CompressionTest class to benchmark
     * and test the ZSTD compression capabilities compared to other compression algorithms like
     * LZ4 and ZLIB.
     */
    private Compressor zstd;
    /**
     *
     */
    private Compressor zlib;
    /**
     * The LZ4 compressor instance used for compressing and decompressing data
     * with the LZ4 algorithm in the CompressionTest class. This field is
     * initialized via the CompressorFactory and is specifically used in tests
     * related to the LZ4 compression method.
     */
    private Compressor lz4;

    /**
     * Sets up the testing environment before each test.
     * This method initializes the compression level and obtains instances
     * of different compressors using the {@link CompressorFactory}.
     * <p>
     * Fields initialized:
     * - {@code level}: Set to 5.
     * - {@code zstd}: Initialized with a ZSTD compressor.
     * - {@code zlib}: Initialized with a ZLIB compressor.
     * - {@code lz4}: Initialized with an LZ4 compressor.
     */
    @Before
    public void setUp() {
        level = 5;
        zstd = CompressorFactory.getCompressor(CompressionType.ZSTD);
        zlib = CompressorFactory.getCompressor(CompressionType.ZLIB);
        lz4 = CompressorFactory.getCompressor(CompressionType.LZ4);
    }

    /**
     * Tests the ZLIB compression algorithm by verifying the following:
     * 1. Matching string representations ("zlib", "ZLiB", "ZLIB") to the CompressionType.ZLIB enum.
     * 2. Compressing and decompressing randomly generated strings of varying lengths.
     * 3. Ensuring that the decompressed byte arrays match the original input.
     *
     * @throws IOException if an I/O error occurs during compression or decompression.
     */
    @Test
    public void testCompressionZlib() throws IOException {
        Assert.assertEquals(CompressionType.ZLIB, CompressionType.of("zlib"));
        Assert.assertEquals(CompressionType.ZLIB, CompressionType.of("ZLiB"));
        Assert.assertEquals(CompressionType.ZLIB, CompressionType.of("ZLIB"));

        int randomKB = 4096;
        Random random = new Random();
        for (int i = 0; i < 5; ++i) {
            String message = RandomStringUtils.randomAlphanumeric(random.nextInt(randomKB) * 1024);
            byte[] srcBytes = message.getBytes(StandardCharsets.UTF_8);
            byte[] compressed = zlib.compress(srcBytes, level);
            byte[] decompressed = zlib.decompress(compressed, 5);
            Assert.assertEquals(decompressed.length, srcBytes.length);
            Assert.assertEquals(new String(decompressed), message);
        }
    }

    /**
     * Tests the Zstandard (ZSTD) compression algorithm to ensure correct functionality.
     * <p>
     * This test method performs the following actions:
     * 1. Validates that various string representations of the "zstd" compression type
     *    correctly map to the CompressionType.ZSTD enum constant.
     * 2. Generates random alphanumeric strings of variable sizes up to 4096 KB.
     * 3. Compresses each generated string using the ZSTD compression algorithm.
     * 4. Decompresses the compressed data and verifies that the decompressed output
     *    matches the original input string.
     *
     * @throws IOException if the compression or decompression processes encounter an I/O error.
     */
    @Test
    public void testCompressionZstd() throws IOException {
        Assert.assertEquals(CompressionType.ZSTD, CompressionType.of("zstd"));
        Assert.assertEquals(CompressionType.ZSTD, CompressionType.of("ZStd"));
        Assert.assertEquals(CompressionType.ZSTD, CompressionType.of("ZSTD"));

        int randomKB = 4096;
        Random random = new Random();
        for (int i = 0; i < 5; ++i) {
            String message = RandomStringUtils.randomAlphanumeric(random.nextInt(randomKB) * 1024);
            byte[] srcBytes = message.getBytes(StandardCharsets.UTF_8);
            byte[] compressed = zstd.compress(srcBytes, level);
            byte[] decompressed = zstd.decompress(compressed, 5);
            Assert.assertEquals(decompressed.length, srcBytes.length);
            Assert.assertEquals(new String(decompressed), message);
        }
    }

    /**
     * Tests the LZ4 compression and decompression process.
     * <p>
     * This method verifies that the LZ4 compression algorithm can correctly compress
     * and decompress a randomly generated alphanumeric string. It asserts that the
     * size and content of the decompressed data matches the original uncompressed data.
     *
     * @throws IOException if an I/O error occurs during compression or decompression
     */
    @Test
    public void testCompressionLz4() throws IOException {
        Assert.assertEquals(CompressionType.LZ4, CompressionType.of("lz4"));
        int randomKB = 4096;
        Random random = new Random();
        for (int i = 0; i < 5; ++i) {
            String message = RandomStringUtils.randomAlphanumeric(random.nextInt(randomKB) * 1024);
            byte[] srcBytes = message.getBytes(StandardCharsets.UTF_8);
            byte[] compressed = lz4.compress(srcBytes, level);
            byte[] decompressed = lz4.decompress(compressed, 5);
            Assert.assertEquals(decompressed.length, srcBytes.length);
            Assert.assertEquals(new String(decompressed), message);
        }
    }

    /**
     * Tests the behavior of the `CompressionType.of(String type)` method when an unsupported compression type is provided.
     * This test passes if a {@link RuntimeException} is thrown, indicating that the input type is not a recognized compression type.
     *
     * @throws RuntimeException if the specified compression type name is not supported
     */
    @Test(expected = RuntimeException.class)
    public void testCompressionUnsupportedType() {
        CompressionType.of("snappy");
    }
}
