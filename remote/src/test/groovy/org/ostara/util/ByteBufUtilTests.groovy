package org.ostara.util

import io.netty.buffer.Unpooled
import spock.lang.Specification
import spock.lang.Stepwise
import spock.lang.Subject

import java.nio.charset.StandardCharsets

import static org.ostara.remote.util.ByteBufUtils.*

@Stepwise
@Subject(ByteBufUtilTests)
class ByteBufUtilTests extends Specification {


    def "buf 2 string."() {
        given:
        def data = Unpooled.copiedBuffer("test-string", StandardCharsets.UTF_8);
        def r = buf2String(data, data.readableBytes());


        when:
        r == null
        then:
        r.length() == 11

        when:
        r != null
        then:
        r.length() == data.readableBytes()

        cleanup:
        release(data);
    }

    def "string 2 buf."() {
        given:
        def s = "test-string";
        def buf = string2Buf(s);

        when:
        buf == null
        then:
        s.length() == 11

        when:
        buf != null
        then:
        s.length() == buf.readableBytes()

        cleanup:
        release(buf);
    }

    def "default if null."() {
        given:
        def s = "test"

        when:
        def ifNull = defaultIfNull(null, s)
        then:
        ifNull == s

        when:
        def ifNotNull = defaultIfNull(s, s)
        then:
        ifNotNull == s
    }
}
