package org.meteor.proxy.beans;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.name.Named;
import com.google.inject.name.Names;
import org.meteor.core.CoreConfig;
import org.meteor.core.MeteorServer;
import org.meteor.listener.MetricsListener;
import org.meteor.management.Manager;
import org.meteor.net.CoreSocketServer;
import org.meteor.net.ServiceChannelInitializer;
import org.meteor.net.ServiceDuplexHandler;
import org.meteor.net.ServiceProcessor;
import org.meteor.proxy.ProxyMetricsListener;
import org.meteor.proxy.management.ZookeeperProxyManager;
import org.meteor.proxy.net.ProxyServerChannelInitializer;
import org.meteor.proxy.net.ProxyServiceProcessor;
import java.util.Properties;

public class ProxyModule extends AbstractModule {
    private final Properties pro;

    public ProxyModule(Properties pro) {
        this.pro = pro;
    }

    @Override
    protected void configure() {
        bind(Manager.class).to(ZookeeperProxyManager.class).in(Singleton.class);
        bind(CoreSocketServer.class).in(Singleton.class);
        bind(MeteorServer.class).in(Singleton.class);
        bind(ServiceChannelInitializer.class).to(ProxyServerChannelInitializer.class).in(Singleton.class);
        bind(ServiceProcessor.class).annotatedWith(Names.named("ProxyServiceProcessor")).to(ProxyServiceProcessor.class);
    }

    @Provides
    @Singleton
    CoreConfig coreConfig() {
        return CoreConfig.fromProps(pro);
    }

    @Provides
    @Singleton
    MetricsListener metricsListener(CoreConfig config, Manager manager) {
        return new ProxyMetricsListener(pro, config, manager);
    }

    @Provides
    @Singleton
    ServiceDuplexHandler serviceDuplexHandler(Manager manager, @Named("ProxyServiceProcessor") ServiceProcessor serviceProcessor) {
        return new ServiceDuplexHandler(manager, serviceProcessor);
    }
}
