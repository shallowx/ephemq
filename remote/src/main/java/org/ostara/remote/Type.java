package org.ostara.remote;

public enum Type {
    HEARTBEAT("heartbeat", 0), PUSH("push message", 1);

    private final int sequence;
    private final String desc;

    public int sequence() {
        return sequence;
    }

    public String desc() {
        return desc;
    }

    Type(String desc, int sequence) {
        this.sequence = sequence;
        this.desc = desc;
    }
}
