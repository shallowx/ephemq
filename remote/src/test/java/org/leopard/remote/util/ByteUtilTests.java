package org.leopard.remote.util;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.Assert;
import org.junit.Test;

import java.nio.charset.StandardCharsets;

import static org.leopard.remote.util.ByteBufUtils.*;

public class ByteUtilTests {

    @Test
    public void testBuf2String() {
        final ByteBuf data = Unpooled.copiedBuffer("test-string", StandardCharsets.UTF_8);
        String r = buf2String(data, data.readableBytes());
        Assert.assertEquals( r == null ? 0 : r.length(), data.readableBytes());
        release(data);
    }

    @Test
    public void testString2Buf() {
        final String var = "test-string";
        final ByteBuf buf = string2Buf(var);

        Assert.assertEquals(var.length(), null == buf ? 0 : buf.readableBytes());
        release(buf);
    }

    @Test
    public void testDefaultIfNull() {
        Assert.assertEquals("test", defaultIfNull(null, "test"));
        Assert.assertEquals("test", defaultIfNull("test", "test-null"));
    }
}
