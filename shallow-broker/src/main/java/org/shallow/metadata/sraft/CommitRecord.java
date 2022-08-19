package org.shallow.metadata.sraft;

import com.google.protobuf.MessageLite;

public class CommitRecord<T> {
    private final T record;
    private final CommitType type;
    private MessageLite request;
    private MessageLite response;

    public CommitRecord(T record, CommitType type) {
        this.record = record;
        this.type = type;
    }

    public CommitRecord(T record, CommitType type, MessageLite request, MessageLite response) {
        this.record = record;
        this.type = type;
        this.request = request;
        this.response = response;
    }

    public CommitType getType() {
        return type;
    }

    public T getRecord() {
        return record;
    }

    public MessageLite getRequest() {
        return request;
    }

    public MessageLite getResponse() {
        return response;
    }

    @Override
    public String toString() {
        return "CommitRecord{" +
                "record=" + record +
                ", type=" + type +
                ", request=" + request +
                ", response=" + response +
                '}';
    }
}
