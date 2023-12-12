package org.meteor.client.internal;

import io.netty.buffer.ByteBuf;
import io.netty.util.AbstractReferenceCounted;
import io.netty.util.ReferenceCounted;
import io.netty.util.internal.ObjectPool;
import org.meteor.common.util.MessageUtils;

public class MessageWrapper extends AbstractReferenceCounted {

    private static final ObjectPool<MessageWrapper> RECYCLER = ObjectPool.newPool(MessageWrapper::new);
    private int ledger;
    private ByteBuf message;
    private ObjectPool.Handle<MessageWrapper> handle;

    private MessageWrapper() {
    }

    public MessageWrapper(ObjectPool.Handle<MessageWrapper> handle) {
        this.handle = handle;
    }

    public static MessageWrapper newInstance(int ledger, ByteBuf message) {
        MessageWrapper msg = RECYCLER.get();
        msg.setRefCnt(1);
        msg.setMessage(message);
        msg.setLedger(ledger);

        return msg;
    }

    public int getEpoch() {
        return MessageUtils.getEpoch(message);
    }

    public int getIndex() {
        return MessageUtils.getIndex(message);
    }

    public int getMarker() {
        return MessageUtils.getMarker(message);
    }

    public ByteBuf getBody() {
        return MessageUtils.getBody(message);
    }

    public ByteBuf getMeta() {
        return MessageUtils.getMeta(message);
    }

    public int getLedger() {
        return ledger;
    }

    public void setLedger(int ledger) {
        this.ledger = ledger;
    }

    public ByteBuf getMessage() {
        return message;
    }

    public void setMessage(ByteBuf message) {
        this.message = message;
    }

    @Override
    protected void deallocate() {
        if (handle == null) {
            return;
        }
        this.ledger = 0;
        if (message != null) {
            message.release();
            message = null;
        }
        this.handle.recycle(this);
    }

    @Override
    public ReferenceCounted touch(Object hint) {
        if (message != null) {
            message.touch(hint);
        }
        return this;
    }
}
