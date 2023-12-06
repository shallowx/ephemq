package org.ostara.proxy.net;

import com.google.inject.Inject;
import org.ostara.beans.CoreConfig;
import org.ostara.management.Manager;
import org.ostara.net.ServiceChannelInitializer;
import org.ostara.net.ServiceDuplexHandler;
import org.ostara.proxy.beans.ProxyBean;

public class ProxyServerChannelInitializer extends ServiceChannelInitializer {

    @Inject
    public ProxyServerChannelInitializer(CoreConfig config, Manager manager) {
        super(config, manager);
    }

    @Override
    protected ServiceDuplexHandler getServiceDuplexHandler() {
        return ProxyBean.getBean(ServiceDuplexHandler.class);
    }
}
