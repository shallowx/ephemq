package org.shallow;

public enum Type {
     HEARTBEAT("heartbeat", 0), PULL("pull message", 1), PUSH("push message",  2);

    private final int sequence;

    public int sequence() {
        return sequence;
    }

    Type(String doc, int sequence) {
        this.sequence = sequence;
    }
}
