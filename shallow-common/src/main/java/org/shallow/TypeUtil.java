package org.shallow;

import javax.naming.OperationNotSupportedException;

public class TypeUtil {

    private TypeUtil() throws OperationNotSupportedException {
        throw new OperationNotSupportedException();
    }

    public static String object2String(Object v) {
        return (String) v;
    }

    public static int object2Int(Object v) {
        return (v instanceof String) ? Integer.parseInt((String) v) : (int) v;
    }

    public static long object2Long(Object v) {
        return (v instanceof String) ? Long.parseLong((String) v) : (long) v;
    }

    public static double object2Double(Object v) {
        return (v instanceof String) ? Double.parseDouble((String) v) : (double) v;
    }

    public static float object2Float(Object v) {
        return (v instanceof String) ? Float.parseFloat((String) v) : (float) v;
    }

    public static boolean object2Boolean(Object v) {
        return (v instanceof String) ? Boolean.parseBoolean((String) v) : (boolean) v;
    }
}
