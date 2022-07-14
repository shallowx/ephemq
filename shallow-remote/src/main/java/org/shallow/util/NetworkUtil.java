package org.shallow.util;

import io.netty.buffer.ByteBuf;
import org.shallow.codec.MessagePacket;

public final class NetworkUtil {

    public static MessagePacket newSuccessPacket(int rejoin, ByteBuf body) {
        return MessagePacket.newPacket(rejoin, (byte) 0, body);
    }

    public static MessagePacket newFailurePacket(int rejoin, Throwable cause) {
        return null;
    }
}
