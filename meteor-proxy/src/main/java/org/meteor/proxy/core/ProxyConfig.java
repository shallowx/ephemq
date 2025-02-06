package org.meteor.proxy.core;

import io.netty.util.NettyRuntime;
import org.meteor.config.CommonConfig;
import org.meteor.config.ZookeeperConfig;

import java.util.Properties;

import static org.meteor.common.util.ObjectLiteralsTransformUtil.object2Int;
import static org.meteor.common.util.ObjectLiteralsTransformUtil.object2String;

/**
 * The ProxyConfig class is responsible for managing and providing configuration
 * settings for a proxy server. It uses a combination of properties, common configuration,
 * and Zookeeper configuration to offer a range of configurable parameters.
 * <p>
 * This class includes methods to retrieve specific configuration values like sync periods,
 * timeouts, and thresholds. It ensures default values for all the required properties and
 * provides methods to access those configuration values.
 * <p>
 * Fields:
 * - `PROXY_UPSTREAM_SERVERS`: The upstream servers the proxy should connect to.
 * - `PROXY_HEAVY_LOAD_SUBSCRIBER_THRESHOLD`: Threshold for heavy load subscribers.
 * - `PROXY_CLIENT_WORKER_THREAD_LIMIT`: Maximum number of client worker threads.
 * - `PROXY_CLIENT_POOL_SIZE`: The size of the client pool.
 * - `PROXY_LEDGER_SYNC_INITIAL_DELAY_MILLISECONDS`: Initial delay for ledger synchronization.
 * - `PROXY_LEDGER_SYNC_PERIOD_MILLISECONDS`: Period for ledger synchronization.
 * - `PROXY_LEDGER_SYNC_SEMAPHORE`: Semaphore value for ledger synchronization.
 * - `PROXY_LEDGER_SYNC_UPSTREAM_TIMEOUT_MILLISECONDS`: Timeout for upstream ledger synchronization.
 * - `PROXY_CHANNEL_CONNECTION_TIMEOUT_MILLISECONDS`: Timeout for channel connection.
 * - `PROXY_RESUME_TASK_SCHEDULE_DELAY_MILLISECONDS`: Schedule delay for resuming tasks.
 * - `PROXY_SYNC_CHECK_INTERVAL_MILLISECONDS`: Interval for checking synchronization.
 * - `PROXY_TOPIC_CHANGE_DELAY_MILLISECONDS`: Delay for topic changes.
 * <p>
 * Methods:
 * - `ProxyConfig(Properties prop, CommonConfig configuration, ZookeeperConfig zookeeperConfiguration)`: Constructor to initialize ProxyConfig with the provided properties, common
 * configuration, and Zookeeper configuration.
 * - `getZookeeperConfiguration()`: Retrieves the Zookeeper configuration.
 * - `getCommonConfiguration()`: Retrieves the common configuration.
 * - `getProxyLeaderSyncPeriodMilliseconds()`: Retrieves the ledger sync period in milliseconds.
 * - `getProxyLeaderSyncSemaphore()`: Retrieves the ledger sync semaphore value.
 * - `getProxyLeaderSyncUpstreamTimeoutMilliseconds()`: Retrieves the timeout for upstream ledger synchronization in milliseconds.
 * - `getProxyChannelConnectionTimeoutMilliseconds()`: Retrieves the channel connection timeout in milliseconds.
 * - `getProxyResumeTaskScheduleDelayMilliseconds()`: Retrieves the schedule delay for resuming tasks in milliseconds.
 * - `getProxySyncCheckIntervalMilliseconds()`: Retrieves the check interval for synchronization in milliseconds.
 * - `getProxyTopicChangeDelayMilliseconds()`: Retrieves the delay for topic changes in milliseconds.
 * - `getProxyUpstreamServers()`: Retrieves the upstream servers.
 * - `getProxyHeavyLoadSubscriberThreshold()`: Retrieves the threshold for heavy load subscribers.
 * - `getProxyClientWorkerThreadLimit()`: Retrieves the limit for client worker threads.
 * - `getProxyClientPoolSize()`: Retrieves the client pool size.
 * - `getProxyLeaderSyncInitialDelayMilliseconds()`: Retrieves the initial delay for ledger synchronization in milliseconds.
 */
public class ProxyConfig {
    /**
     * The configuration property key for specifying the upstream servers
     * that the proxy will connect to.
     * <p>
     * This key is used to retrieve a list or comma-separated string of
     * upstream server addresses that the proxy will use to forward requests.
     */
    private static final String PROXY_UPSTREAM_SERVERS = "proxy.upstream.servers";
    /**
     * Configuration property key for determining the threshold of heavy load for a subscriber
     * in the proxy server.
     * <p>
     * This setting specifies a value which, when exceeded, indicates that the proxy server
     * is experiencing heavy load conditions for subscriber activities. It is used to apply
     * load management strategies and ensure optimal performance under high-load scenarios.
     */
    private static final String PROXY_HEAVY_LOAD_SUBSCRIBER_THRESHOLD = "proxy.heavy.load.subscriber.threshold";
    /**
     * Configuration key representing the maximum number of worker threads
     * that can be utilized by the proxy client.
     * This limit is used to prevent the proxy client from exceeding
     * the specified number of concurrent worker threads.
     */
    private static final String PROXY_CLIENT_WORKER_THREAD_LIMIT = "proxy.client.worker.thread.limit";
    /**
     * The configuration property key for setting the size of the proxy client's connection pool.
     * This key is used to define the maximum number of connections that can be maintained in the pool
     * for proxy clients, ensuring efficient resource usage and connection management.
     */
    private static final String PROXY_CLIENT_POOL_SIZE = "proxy.client.pool.size";
    /**
     * Configuration property key for the initial delay in milliseconds before the ledger sync process starts.
     * <p>
     * This setting defines the initial delay (in milliseconds) before starting the ledger synchronization
     * process. It can be used to stagger the start of the ledger sync to avoid overloading the system at startup.
     */
    private static final String PROXY_LEDGER_SYNC_INITIAL_DELAY_MILLISECONDS = "proxy.ledger.sync.initial.delay.milliseconds";
    /**
     * A constant property key representing the synchronization period for the proxy ledger.
     * This key is used to determine the time interval in milliseconds between automatic
     * sync operations of the proxy ledger with the upstream server.
     */
    private static final String PROXY_LEDGER_SYNC_PERIOD_MILLISECONDS = "proxy.ledger.sync.period.milliseconds";
    /**
     * Configuration key for the semaphore used to synchronize ledger operations in the proxy.
     * This semaphore helps to control concurrent access to the ledger, ensuring that the
     * synchronization process is properly managed and does not overwhelm system resources.
     */
    private static final String PROXY_LEDGER_SYNC_SEMAPHORE = "proxy.ledger.sync.semaphore";
    /**
     * Configuration property key representing the timeout duration (in milliseconds)
     * for synchronizing the proxy ledger with an upstream server. This timeout value
     * determines the maximum amount of time the system will wait for a successful
     * synchronization response from upstream servers before considering the attempt as failed.
     */
    private static final String PROXY_LEDGER_SYNC_UPSTREAM_TIMEOUT_MILLISECONDS = "proxy.ledger.sync.upstream.timeout.milliseconds";
    /**
     * Configuration property key that defines the timeout duration in milliseconds for establishing a connection
     * to the proxy channel. This value specifies the maximum time the system will wait for a connection to be
     * established before timing out.
     */
    private static final String PROXY_CHANNEL_CONNECTION_TIMEOUT_MILLISECONDS = "proxy.channel.connection.timeout.milliseconds";
    /**
     * String constant for configuring the delay in milliseconds before scheduling
     * a resume task for the proxy.
     * <p>
     * This configuration setting is used to manage and control the scheduling delay
     * for resuming tasks in the proxy service.
     */
    private static final String PROXY_RESUME_TASK_SCHEDULE_DELAY_MILLISECONDS = "proxy.resume.task.schedule.delay.milliseconds";
    /**
     * Configuration key representing the interval in milliseconds at which the proxy synchronization check occurs.
     * This value dictates how frequently the system checks for synchronization status, updates, or potential issues
     * in the proxy configuration. It impacts the responsiveness and consistency of the proxy service with respect
     * to upstream changes and downstream requests.
     */
    private static final String PROXY_SYNC_CHECK_INTERVAL_MILLISECONDS = "proxy.sync.check.interval.milliseconds";
    /**
     * Configuration property key for the delay in milliseconds before processing a topic change in the proxy.
     * <p>
     * This variable defines the duration to wait before handling changes to topics, allowing for any
     * necessary initialization or synchronization processes to complete.
     */
    private static final String PROXY_TOPIC_CHANGE_DELAY_MILLISECONDS = "proxy.topic.change.delay.milliseconds";
    /**
     * Holds configuration properties for the ProxyConfig class.
     * <p>
     * This property is a collection of key-value pairs that provide
     * the necessary configuration settings for the ProxyConfig.
     * Once initialized through the constructor, these properties
     * cannot be modified.
     */
    private final Properties prop;
    /**
     * Holds the common configuration parameters for the proxy server.
     * <p>
     * This variable provides access to various configuration settings
     * that are shared across different components of the proxy server.
     */
    private final CommonConfig commonConfiguration;
    /**
     * Holds the configuration settings for connecting to the Zookeeper ensemble.
     * <p>
     * This instance provides the necessary Zookeeper configuration parameters such as
     * the Zookeeper URL, connection retry settings, connection timeout, and session timeout.
     */
    private final ZookeeperConfig zookeeperConfiguration;

    /**
     * Constructs a ProxyConfig instance with the specified properties, common configuration, and Zookeeper configuration.
     *
     * @param prop the properties to be used for proxy configuration
     * @param configuration the common configuration settings
     * @param zookeeperConfiguration the Zookeeper configuration settings
     */
    public ProxyConfig(Properties prop, CommonConfig configuration, ZookeeperConfig zookeeperConfiguration) {
        this.prop = prop;
        this.commonConfiguration = configuration;
        this.zookeeperConfiguration = zookeeperConfiguration;
    }

    /**
     * Retrieves the Zookeeper configuration.
     *
     * @return the current {@link ZookeeperConfig} instance containing the Zookeeper configuration settings.
     */
    public ZookeeperConfig getZookeeperConfiguration() {
        return zookeeperConfiguration;
    }

    /**
     * Retrieves the common configuration settings for the proxy.
     *
     * @return the common configuration settings encapsulated in a {@code CommonConfig} object
     */
    public CommonConfig getCommonConfiguration() {
        return commonConfiguration;
    }

    /**
     * Returns the synchronization period for the proxy leader in milliseconds.
     *
     * @return the proxy leader sync period in milliseconds, or the default value of 60000 milliseconds if not specified.
     */
    public int getProxyLeaderSyncPeriodMilliseconds() {
        return object2Int(prop.getOrDefault(PROXY_LEDGER_SYNC_PERIOD_MILLISECONDS, 60000));
    }

    /**
     * Retrieves the configured semaphore value used for synchronizing the proxy leader.
     * If the value is not explicitly set, a default value of 100 is returned.
     *
     * @return the semaphore value for proxy leader synchronization, defaulting to 100 if not set
     */
    public int getProxyLeaderSyncSemaphore() {
        return object2Int(prop.getOrDefault(PROXY_LEDGER_SYNC_SEMAPHORE, 100));
    }

    /**
     * Retrieves the timeout duration in milliseconds for proxy leader synchronization with
     * the upstream. If the value is not explicitly set, a default value of 1900 milliseconds is used.
     *
     * @return the timeout duration in milliseconds for proxy leader sync upstream
     */
    public int getProxyLeaderSyncUpstreamTimeoutMilliseconds() {
        return object2Int(prop.getOrDefault(PROXY_LEDGER_SYNC_UPSTREAM_TIMEOUT_MILLISECONDS, 1900));
    }

    /**
     * Returns the proxy channel connection timeout in milliseconds.
     * If the property is not set, it returns a default value of 3000 milliseconds.
     *
     * @return the timeout value in milliseconds for proxy channel connections
     */
    public int getProxyChannelConnectionTimeoutMilliseconds() {
        return object2Int(prop.getOrDefault(PROXY_CHANNEL_CONNECTION_TIMEOUT_MILLISECONDS, 3000));
    }

    /**
     * Retrieves the delay in milliseconds before scheduling the proxy resume task.
     *
     * @return the delay in milliseconds for scheduling the proxy resume task; returns a default value of 3000 milliseconds if not specified.
     */
    public int getProxyResumeTaskScheduleDelayMilliseconds() {
        return object2Int(prop.getOrDefault(PROXY_RESUME_TASK_SCHEDULE_DELAY_MILLISECONDS, 3000));
    }

    /**
     * Retrieves the interval at which the proxy synchronization check is performed, in milliseconds.
     * If the property {@code PROXY_SYNC_CHECK_INTERVAL_MILLISECONDS} is not set, a default value of 5000 milliseconds is used.
     *
     * @return the proxy synchronization check interval in milliseconds
     */
    public int getProxySyncCheckIntervalMilliseconds() {
        return object2Int(prop.getOrDefault(PROXY_SYNC_CHECK_INTERVAL_MILLISECONDS, 5000));
    }

    /**
     * Retrieves the delay in milliseconds before changing the proxy topic.
     *
     * @return the delay in milliseconds before a proxy topic change occurs.
     *         If the property is not set, the default value of 15000 milliseconds is returned.
     */
    public int getProxyTopicChangeDelayMilliseconds() {
        return object2Int(prop.getOrDefault(PROXY_TOPIC_CHANGE_DELAY_MILLISECONDS, 15000));
    }

    /**
     * Retrieves the proxy upstream servers from the configuration properties.
     * If the value is not set, it defaults to "127.0.0.1:9527".
     *
     * @return a comma-separated string of proxy upstream server addresses
     */
    public String getProxyUpstreamServers() {
        return object2String(prop.getOrDefault(PROXY_UPSTREAM_SERVERS, "127.0.0.1:9527"));
    }

    /**
     * Retrieves the threshold value for determining if a proxy is under heavy load based on the number of subscribers.
     *
     * @return The threshold value as an integer for identifying a proxy under heavy load. Default value is 200000 if not set.
     */
    public int getProxyHeavyLoadSubscriberThreshold() {
        return object2Int(prop.getOrDefault(PROXY_HEAVY_LOAD_SUBSCRIBER_THRESHOLD, 200000));
    }

    /**
     * Retrieves the limit on the number of worker threads for the proxy client.
     * It returns the value defined by the property PROXY_CLIENT_WORKER_THREAD_LIMIT,
     * or the number of available processors if the property is not set.
     *
     * @return the limit on the number of worker threads for the proxy client.
     */
    public int getProxyClientWorkerThreadLimit() {
        return object2Int(prop.getOrDefault(PROXY_CLIENT_WORKER_THREAD_LIMIT, NettyRuntime.availableProcessors()));
    }

    /**
     * Retrieves the size of the proxy client pool.
     *
     * @return the size of the proxy client pool as an integer. If the PROXY_CLIENT_POOL_SIZE is not set, the default value of 3 is returned.
     */
    public int getProxyClientPoolSize() {
        return object2Int(prop.getOrDefault(PROXY_CLIENT_POOL_SIZE, 3));
    }

    /**
     * Retrieves the initial delay in milliseconds before the proxy leader synchronization task starts.
     *
     * @return the initial delay in milliseconds for the proxy leader synchronization task
     */
    public int getProxyLeaderSyncInitialDelayMilliseconds() {
        return object2Int(prop.getOrDefault(PROXY_LEDGER_SYNC_INITIAL_DELAY_MILLISECONDS, 60000));
    }
}
