package org.shallow.handle;

import org.shallow.log.Cursor;
import org.shallow.log.Offset;

public class EntryTrace {
    private EntrySubscription subscription;
    private Cursor cursor;
    private Offset offset;
    private int traceLimit;
    private int alignLimit;

    private EntryTrace() {
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

    public int getTraceLimit() {
        return traceLimit;
    }

    public int getAlignLimit() {
        return alignLimit;
    }

    public static TraceBuilder newBuilder() {
        return new TraceBuilder();
    }

    public static class TraceBuilder {
        EntrySubscription subscription;
        Cursor cursor;
        Offset offset;
        int traceLimit;
        int alignLimit;

        private TraceBuilder() {
        }

        public TraceBuilder subscription(EntrySubscription subscription) {
            this.subscription = subscription;
            return this;
        }

        public TraceBuilder cursor(Cursor cursor) {
            this.cursor = cursor;
            return this;
        }


        public TraceBuilder traceLimit(int traceLimit) {
            this.traceLimit = traceLimit;
            return this;
        }

        public TraceBuilder alignLimit(int alignLimit) {
            this.alignLimit = alignLimit;
            return this;
        }

        public TraceBuilder offset(Offset offset) {
            this.offset = offset;
            return this;
        }

        public EntryTrace build() {
            EntryTrace trace = new EntryTrace();

            trace.subscription = this.subscription;
            trace.cursor = this.cursor;
            trace.offset = this.offset;
            trace.traceLimit = this.traceLimit;
            trace.alignLimit = this.alignLimit;

            return trace;
        }
    }

    @Override
    public String toString() {
        return "EntryTrace{" +
                "subscription=" + subscription +
                ", cursor=" + cursor +
                ", offset=" + offset +
                ", traceLimit=" + traceLimit +
                ", alignLimit=" + alignLimit +
                '}';
    }
}
