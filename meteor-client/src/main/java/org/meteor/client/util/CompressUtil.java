package org.meteor.client.util;

import io.netty.buffer.ByteBuf;
import org.meteor.remote.util.ByteBufUtil;

public class CompressUtil {
    public void compress() {

    }

    public void unCompress() {

    }

    public boolean needCompress(ByteBuf buf) {
        int length = ByteBufUtil.bufLength(buf);
        return length >= MessageConstants.MESSAGE_MAX_LENGTH;
    }
}
