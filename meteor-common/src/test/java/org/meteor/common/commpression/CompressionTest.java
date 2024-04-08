package org.meteor.common.commpression;

import static org.junit.Assert.assertThat;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Random;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.meteor.common.compression.CompressionType;
import org.meteor.common.compression.Compressor;
import org.meteor.common.compression.CompressorFactory;

public class CompressionTest {
    private int level;
    private Compressor zstd;
    private Compressor zlib;
    private Compressor lz4;

    @Before
    public void setUp() {
        level = 5;
        zstd = CompressorFactory.getCompressor(CompressionType.ZSTD);
        zlib = CompressorFactory.getCompressor(CompressionType.ZLIB);
        lz4 = CompressorFactory.getCompressor(CompressionType.LZ4);
    }

    @Test
    public void testCompressionZlib() throws IOException {
        Assert.assertEquals(CompressionType.of("zlib"), CompressionType.ZLIB);
        Assert.assertEquals(CompressionType.of("ZLiB"), CompressionType.ZLIB);
        Assert.assertEquals(CompressionType.of("ZLIB"), CompressionType.ZLIB);

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

    @Test
    public void testCompressionZstd() throws IOException {
        Assert.assertEquals(CompressionType.of("zstd"), CompressionType.ZSTD);
        Assert.assertEquals(CompressionType.of("ZStd"), CompressionType.ZSTD);
        Assert.assertEquals(CompressionType.of("ZSTD"), CompressionType.ZSTD);

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

    @Test
    public void testCompressionLz4() throws IOException {
        Assert.assertEquals(CompressionType.of("lz4"), CompressionType.LZ4);
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

    @Test(expected = RuntimeException.class)
    public void testCompressionUnsupportedType() {
        CompressionType.of("snappy");
    }
}
