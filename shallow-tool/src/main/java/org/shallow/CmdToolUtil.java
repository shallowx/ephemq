package org.shallow;

import java.text.SimpleDateFormat;
import java.util.Date;

public class CmdToolUtil {
    private static final SimpleDateFormat FORMAT = new SimpleDateFormat("HH:mm:ss.SSS");

    public static String newDate() {
        return FORMAT.format(new Date());
    }

    public static String currentThread() {
        return Thread.currentThread().getName();
    }

    public static String className(Class<?> clz) {
        return clz.getName();
    }
}
