package org.shallow.handle;

import org.shallow.ledger.Cursor;
import org.shallow.ledger.Offset;

public class EntryAttributes {
    private EntrySubscription subscription;
    private Cursor cursor;
    private Offset offset;
    private int assignLimit;
    private int alignLimit;

    private EntryAttributes() {
        //unsupported
    }

    public EntrySubscription getSubscription() {
        return subscription;
    }

    public Cursor getCursor() {
        return cursor;
    }

    public Offset getOffset() {
        return offset;
    }

    public void setOffset(Offset offset) {
        this.offset = offset;
    }

    public int getAssignLimit() {
        return assignLimit;
    }

    public int getAlignLimit() {
        return alignLimit;
    }

    public static AttributeBuilder newBuilder() {
        return new AttributeBuilder();
    }

    public static class AttributeBuilder {
        EntrySubscription subscription;
        Cursor cursor;
        Offset offset;
        int assignLimit;
        int alignLimit;

        private AttributeBuilder() {
        }

        public AttributeBuilder subscription(EntrySubscription subscription) {
            this.subscription = subscription;
            return this;
        }

        public AttributeBuilder cursor(Cursor cursor) {
            this.cursor = cursor;
            return this;
        }


        public AttributeBuilder assignLimit(int assignLimit) {
            this.assignLimit = assignLimit;
            return this;
        }

        public AttributeBuilder alignLimit(int alignLimit) {
            this.alignLimit = alignLimit;
            return this;
        }

        public AttributeBuilder offset(Offset offset) {
            this.offset = offset;
            return this;
        }

        public EntryAttributes build() {
            EntryAttributes attributes = new EntryAttributes();

            attributes.subscription = this.subscription;
            attributes.cursor = this.cursor;
            attributes.offset = this.offset;
            attributes.assignLimit = this.assignLimit;
            attributes.alignLimit = this.alignLimit;

            return attributes;
        }
    }

    @Override
    public String toString() {
        return "EntryTrace{" +
                "subscription=" + subscription +
                ", cursor=" + cursor +
                ", offset=" + offset +
                ", assignLimit=" + assignLimit +
                ", alignLimit=" + alignLimit +
                '}';
    }
}
