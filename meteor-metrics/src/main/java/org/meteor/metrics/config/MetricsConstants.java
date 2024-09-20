package org.meteor.metrics.config;

/**
 * Holds the constant definitions for various metrics that are used in the system.
 */
public class MetricsConstants {
    /**
     * The name of the gauge metric used to monitor the count of active channels in the system.
     */
    public static final String ACTIVE_CHANNEL_GAUGE_NAME = "active_channel_count";
    /**
     * A constant that holds the name of the metric used to summarize the size
     * of the received requests. This metric helps in tracking and monitoring the
     * sizes of incoming requests over time, providing insights into request size
     * distribution and trends.
     */
    public static final String REQUEST_SIZE_SUMMARY_NAME = "received_request_size";
    /**
     * The name of the metric that tracks the response time of API calls.
     */
    public static final String API_RESPONSE_TIME_NAME = "api_response_time";
    /**
     * The name of the gauge metric that tracks the total number of log segments.
     */
    public static final String LOG_SEGMENT_GAUGE_NAME = "log_segment_total";
    /**
     * The name of the gauge metric that tracks the count of log segments.
     */
    public static final String LOG_SEGMENT_COUNT_GAUGE_NAME = "log_segment_count";
    /**
     * Represents the name for the direct memory metric.
     */
    public static final String DIRECT_MEMORY_NAME = "direct_memory";
    /**
     * Metric name for tracking the count of pending Netty tasks.
     */
    public static final String NETTY_PENDING_TASK_NAME = "netty_pending_task";
    /**
     * The name of the metric that counts the state of requests.
     */
    public static final String REQUEST_STATE_COUNTER_NAME = "request_state_count";
    /**
     * A constant that specifies the name of the counter used to track the number of messages
     * received on a specific topic. This is primarily used for monitoring and analytics purposes.
     */
    public static final String TOPIC_MSG_RECEIVE_COUNTER_NAME = "topic_msg_receive_count";
    /**
     * A constant representing the name of the counter that tracks the number of messages
     * pushed to a topic in the system.
     */
    public static final String TOPIC_MSG_PUSH_COUNTER_NAME = "topic_msg_push_count";
    /**
     * A constant string used as the name for the gauge that measures the count of topic partitions.
     */
    public static final String TOPIC_PARTITION_COUNT_GAUGE_NAME = "topic_partition_count";
    /**
     * Metric name representing the gauge for the count of partition leaders within a topic.
     */
    public static final String TOPIC_PARTITION_LEADER_COUNT_GAUGE_NAME = "topic+partition_leader_count";
    /**
     * Represents the name of the summary metric that tracks the count of chunks
     * synchronized by the proxy.
     */
    public static final String PROXY_SYNC_CHUNK_COUNT_SUMMARY_NAME = "proxy_sync_chunk_count";
    /**
     * Represents the tag used to label or identify the result of a metric or operation.
     */
    public static final String RESULT_TAG = "result";
    /**
     * A constant representing the tag for a topic in the metrics system.
     * It is used to identify and categorize metrics that are specific to different topics.
     */
    public static final String TOPIC_TAG = "topic";
    /**
     * A constant tag used to identify the partition in metrics.
     */
    public static final String PARTITION_TAG = "partition";
    /**
     * The tag used to identify the cluster in the metrics system.
     */
    public static final String CLUSTER_TAG = "cluster";
    /**
     * Represents the tag value for broker related metrics.
     * Used to categorize and identify metrics specific to brokers in the system.
     */
    public static final String BROKER_TAG = "broker";
    /**
     * Constant key used to denote the type of a metric, entity, or system component.
     */
    public static final String TYPE_TAG = "type";
    /**
     * This constant represents the tag used for identifying ledger-related metrics in the system.
     */
    public static final String LEDGER_TAG = "ledger";
    /**
     * The constant representing the tag used for naming metrics entities.
     */
    public static final String NAME = "name";
    /**
     * A constant representing the identifier used in various metrics within the system.
     */
    public static final String ID = "id";

}
