package org.meteor.expand.core;

public record Storage(String topic, String queue, int ledger, byte[] message) {
    public static final Storage EMPTY_STORAGE = new Storage(null, null, -1, null);
}
