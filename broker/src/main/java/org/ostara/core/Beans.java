package org.ostara.core;

import com.google.inject.Guice;
import com.google.inject.Injector;

import java.util.Properties;

public class Beans {
    private static Injector injector;

    private static void init(Properties properties) {
        injector = Guice.createInjector(new BeanModule(properties));
    }

    public static <T> T getBean(Class<T> clz) {
        return injector.getInstance(clz);
    }
}
