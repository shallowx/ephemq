package org.shallow.util;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.Assert;
import org.junit.Test;

import java.nio.charset.StandardCharsets;

import static org.shallow.util.ObjectUtil.isNull;
import static org.shallow.util.ByteBufUtil.*;

public class ByteUtilTests {

    @Test
    public void testBuf2String() {
        final ByteBuf data = Unpooled.copiedBuffer("test-string", StandardCharsets.UTF_8);
        String r = buf2String(data, data.readableBytes());
        Assert.assertEquals(isNull(r) ? 0 : r.length(), data.readableBytes());
        release(data);
    }

    @Test
    public void testString2Buf() {
        final String var = "test-string";
        final ByteBuf buf = string2Buf(var);

        Assert.assertEquals(var.length(), isNull(buf) ? 0 : buf.readableBytes());
        release(buf);
    }

    @Test
    public void testDefaultIfNull() {
        Assert.assertEquals("test", defaultIfNull(null, "test"));
        Assert.assertEquals("test", defaultIfNull("test", "test-null"));
    }
}
