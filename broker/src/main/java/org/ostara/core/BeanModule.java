package org.ostara.core;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.name.Named;
import com.google.inject.name.Names;
import org.ostara.listener.MetricsListener;
import org.ostara.management.Manager;
import org.ostara.management.zookeeper.ZookeeperManager;
import org.ostara.network.OstaraSocketServer;
import org.ostara.network.ServiceChannelInitializer;
import org.ostara.network.ServiceDuplexHandler;
import org.ostara.network.ServiceProcessorAware;
import org.ostara.remote.processor.ProcessorAware;

import java.util.Properties;

public class BeanModule extends AbstractModule {

    private final Properties properties;

    public BeanModule(Properties properties) {
        this.properties = properties;
    }

    @Override
    protected void configure() {
        bind(Manager.class).to(ZookeeperManager.class).in(Singleton.class);
        bind(OstaraServer.class).in(Singleton.class);
        bind(ServiceChannelInitializer.class).in(Singleton.class);
        bind(OstaraSocketServer.class).in(Singleton.class);
        bind(ProcessorAware.class).annotatedWith(Names.named("ServiceProcessorAware")).to(ServiceProcessorAware.class);
    }

    @Singleton
    @Provides
    CoreConfig config() {
        return CoreConfig.fromProps(properties);
    }

    @Singleton
    @Provides
    MetricsListener metricsListener(CoreConfig config, Manager manager) {
        return new MetricsListener(properties, config, manager);
    }

    @Provides
    ServiceDuplexHandler serviceDuplexHandler(Manager manager, @Named("ServiceProcessorAware") ProcessorAware aware) {
        return new ServiceDuplexHandler(manager, aware);
    }
}
