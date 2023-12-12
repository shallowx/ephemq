package org.meteor.common.logging;

import org.apache.log4j.Logger;

public class Log4JLoggerFactory extends InternalLoggerFactory {

    public static final InternalLoggerFactory INSTANCE = new Log4JLoggerFactory();

    @Override
    public InternalLogger newLogger(String name) {
        return new Log4JLogger(Logger.getLogger(name));
    }
}
