package org.ostara.core;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.name.Named;
import com.google.inject.name.Names;
import org.ostara.management.Manager;
import org.ostara.management.ZookeeperManager;
import org.ostara.network.DefaultSocketServer;
import org.ostara.network.ServiceChannelInitizalizer;
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
        bind(DefaultServer.class).in(Singleton.class);
        bind(ServiceChannelInitizalizer.class).in(Singleton.class);
        bind(DefaultSocketServer.class).in(Singleton.class);
        bind(ProcessorAware.class).annotatedWith(Names.named("ServiceProcessorAware")).to(ServiceProcessorAware.class);
    }

    @Singleton
    @Provides
    Config config() {
        return Config.fromProps(properties);
    }

    @Singleton
    @Provides
    MetricsListener metricsListener(Config config, Manager manager) {
        return new MetricsListener(properties, config, manager);
    }

    @Provides
    ServiceDuplexHandler serviceDuplexHandler(Manager manager, @Named("ServiceProcessorAware") ProcessorAware aware) {
        return new ServiceDuplexHandler(manager, aware);
    }
}
