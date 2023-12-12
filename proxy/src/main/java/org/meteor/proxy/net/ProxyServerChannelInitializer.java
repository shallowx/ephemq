package org.meteor.proxy.net;

import com.google.inject.Inject;
import org.meteor.core.CoreConfig;
import org.meteor.management.Manager;
import org.meteor.net.ServiceChannelInitializer;
import org.meteor.net.ServiceDuplexHandler;
import org.meteor.proxy.beans.ProxyBean;

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
