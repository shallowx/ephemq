package org.meteor.proxy.beans;

import com.google.inject.Guice;
import com.google.inject.Injector;

import java.util.Properties;

public class ProxyBean {
    private static Injector integer;
    public static void init(Properties pro) {
        integer = Guice.createInjector(new ProxyModule(pro));
    }

    public static <T> T getBean(Class<T> clz) {
        return integer.getInstance(clz);
    }
}
