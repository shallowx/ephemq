package org.ostara.proxy.beans;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.name.Named;
import com.google.inject.name.Names;
import org.ostara.beans.CoreConfig;
import org.ostara.beans.OstaraServer;
import org.ostara.listener.MetricsListener;
import org.ostara.management.Manager;
import org.ostara.net.CoreSocketServer;
import org.ostara.net.ServiceChannelInitializer;
import org.ostara.net.ServiceDuplexHandler;
import org.ostara.net.ServiceProcessor;
import org.ostara.proxy.ProxyMetricsListener;
import org.ostara.proxy.management.ZookeeperProxyManager;
import org.ostara.proxy.net.ProxyServerChannelInitializer;
import org.ostara.proxy.net.ProxyServiceProcessor;
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
        bind(OstaraServer.class).in(Singleton.class);
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
