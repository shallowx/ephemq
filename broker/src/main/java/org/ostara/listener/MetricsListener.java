package org.ostara.listener;

import org.ostara.common.Node;
import org.ostara.common.TopicAssignment;
import org.ostara.common.TopicPartition;
import org.ostara.log.Log;

public class MetricsListener implements APIListener, ServerListener, LogListener, TopicListener, AutoCloseable{
    @Override
    public void close() throws Exception {

    }

    @Override
    public void onCommand(int code, int bytes, long cost, boolean result) {

    }

    @Override
    public void onInitLog(Log log) {

    }

    @Override
    public void onReceiveMessage(String topic, int ledger, int count) {

    }

    @Override
    public void onSyncMessage(String topic, int ledger, int count) {

    }

    @Override
    public void onPushMessage(String topic, int ledger, int count) {

    }

    @Override
    public void onChunkPushMessage(String topic, int ledger, int count) {

    }

    @Override
    public void onStartup(Node node) {

    }

    @Override
    public void onShutdown(Node node) {

    }

    @Override
    public void onPartitionInit(TopicPartition topicPartition, int ledger) {

    }

    @Override
    public void onPartitionDestroy(TopicPartition topicPartition, int ledger) {

    }

    @Override
    public void onPartitionGetLeader(TopicPartition topicPartition) {

    }

    @Override
    public void onPartitionLostLeader(TopicPartition topicPartition) {

    }

    @Override
    public void onTopicCreated(String topic) {

    }

    @Override
    public void onTopicDeleted(String topic) {

    }

    @Override
    public void onPartitionChanged(TopicPartition topicPartition, TopicAssignment oldAssigment, TopicAssignment newAssigment) {

    }
}
