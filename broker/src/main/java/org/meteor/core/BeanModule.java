package org.meteor.core;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.name.Named;
import com.google.inject.name.Names;
import org.meteor.listener.MetricsListener;
import org.meteor.management.Manager;
import org.meteor.management.ZookeeperManager;
import org.meteor.net.CoreSocketServer;
import org.meteor.net.ServiceProcessor;
import org.meteor.net.ServiceChannelInitializer;
import org.meteor.net.ServiceDuplexHandler;
import org.meteor.remote.processor.Processor;

import java.util.Properties;

public class BeanModule extends AbstractModule {

    private final Properties properties;

    public BeanModule(Properties properties) {
        this.properties = properties;
    }

    @Override
    protected void configure() {
        bind(Manager.class).to(ZookeeperManager.class).in(Singleton.class);
        bind(MeteorServer.class).in(Singleton.class);
        bind(ServiceChannelInitializer.class).in(Singleton.class);
        bind(CoreSocketServer.class).in(Singleton.class);
        bind(Processor.class).annotatedWith(Names.named("ServiceProcessorAware")).to(ServiceProcessor.class);
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
    ServiceDuplexHandler serviceDuplexHandler(Manager manager, @Named("ServiceProcessorAware") Processor aware) {
        return new ServiceDuplexHandler(manager, aware);
    }
}
