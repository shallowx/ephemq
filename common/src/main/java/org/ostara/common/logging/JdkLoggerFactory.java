package org.ostara.common.logging;


import java.util.logging.Logger;

public class JdkLoggerFactory extends InternalLoggerFactory {

    public static final InternalLoggerFactory INSTANCE = new JdkLoggerFactory();

    @Override
    public InternalLogger newLogger(String name) {
        return new JdkLogger(Logger.getLogger(name));
    }
}
