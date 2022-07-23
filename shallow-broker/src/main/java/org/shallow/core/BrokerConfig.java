package org.shallow.core;

import org.shallow.metadata.MappingConstants;

import java.util.Properties;

import static org.shallow.TypeUtil.*;

public class BrokerConfig {

    private final Properties config;

    private static final String IO_THREAD_WHOLES = "shallow.io.thread.wholes";
    private static final String WORK_THREAD_WHOLES = "shallow.network.thread.wholes";
    private static final String OS_IS_EPOLL_PREFER= "shallow.os.epoll.prefer";
    private static final String SOCKET_WRITE_HIGH_WATER_MARK = "shallow.socket.write.high.water.mark";
    private static final String EXPOSED_HOST = "shallow.exposed.host";
    private static final String EXPOSED_PORT = "shallow.exposed.port";
    private static final String NETWORK_LOGGING_DEBUG_ENABLED = "network.logging.debug.enabled";
    private static final String TOPICS_METADATA_DIRECTORY = "shallow.topics.metadata.directory";

    public static BrokerConfig exchange(Properties properties) {
        return new BrokerConfig(properties);
    }

    private BrokerConfig(Properties config) {
        this.config = config;
    }

    private int availableProcessor() {
        return Runtime.getRuntime().availableProcessors();
    }

    public int obtainIoThreadWholes(){
        return object2Int(config.getOrDefault(IO_THREAD_WHOLES, 1));
    }

    public int obtainNetworkThreadWholes(){
        return object2Int(config.getOrDefault(WORK_THREAD_WHOLES, availableProcessor()));
    }

    public boolean isOsEpollPrefer(){
        return object2Boolean(config.getOrDefault(OS_IS_EPOLL_PREFER, false));
    }

    public int obtainSocketWriteHighWaterMark(){
        return object2Int(config.getOrDefault(SOCKET_WRITE_HIGH_WATER_MARK, 20 * 1024 * 1024));
    }

    public String obtainExposedHost(){
        return object2String(config.getOrDefault(EXPOSED_HOST, "127.0.0.1"));
    }

    public String obtainTopicsMetadataDirectory(){
        return object2String(config.getOrDefault(TOPICS_METADATA_DIRECTORY, MappingConstants.DIRECTORY));
    }

    public int obtainExposedPort(){
        return object2Int(config.getOrDefault(EXPOSED_PORT, 7730));
    }

    public boolean isNetworkLoggingDebugEnabled() {
        return object2Boolean(config.getOrDefault(NETWORK_LOGGING_DEBUG_ENABLED, false));
    }
}
