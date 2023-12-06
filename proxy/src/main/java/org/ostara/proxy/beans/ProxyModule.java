package org.ostara.proxy.beans;

import com.google.inject.AbstractModule;

import java.util.Properties;

public class ProxyModule extends AbstractModule {
    private final Properties pro;

    public ProxyModule(Properties pro) {
        this.pro = pro;
    }

    @Override
    protected void configure() {
    }
}
