package org.meteor.common.compression;

import java.io.IOException;

public interface Compressor {
    byte[] compress(byte[] src, int level) throws IOException;

    byte[] decompress(byte[] src, int level) throws IOException;
}
