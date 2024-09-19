package org.meteor.common.util;

import javax.naming.OperationNotSupportedException;

/**
 * Utility class for transforming object literals to various primitive and wrapper types.
 */
public class ObjectLiteralsTransformUtil {
    /**
     * Private constructor to prevent instantiation of this utility class.
     *
     * @throws OperationNotSupportedException always thrown to prevent instantiation
     */
    private ObjectLiteralsTransformUtil() throws OperationNotSupportedException {
        throw new OperationNotSupportedException();
    }

    /**
     * Converts the specified object to a string by casting it.
     * This method assumes that the object passed in can be safely cast to a string.
     *
     * @param v the object to be converted to a string
     * @return the string representation of the object
     * @throws ClassCastException if the object cannot be cast to a string
     */
    public static String object2String(Object v) {
        return (String) v;
    }

    /**
     * Converts an object to an integer. If the object is a string, it parses the string to an integer.
     * Otherwise, it casts the object to an integer.
     *
     * @param v the object to be converted to an integer
     * @return the integer value of the object
     */
    public static int object2Int(Object v) {
        return (v instanceof String) ? Integer.parseInt((String) v) : (int) v;
    }

    /**
     *
     */
    public static long object2Long(Object v) {
        return (v instanceof String) ? Long.parseLong((String) v) : (long) v;
    }

    /**
     * Converts an object to a double. If the object is a string, it parses the string to a double.
     * Otherwise, it casts the object to a double.
     *
     * @param v the object to be converted
     * @return the double representation of the object
     */
    public static double object2Double(Object v) {
        return (v instanceof String) ? Double.parseDouble((String) v) : (double) v;
    }

    /**
     * Converts the provided object to a float. If the object is a String, it parses the String as a float.
     * Otherwise, it casts the object to a float.
     *
     * @param v the object to be converted to a float
     * @return the float value represented by the provided object
     * @throws ClassCastException    if the object is not a String or cannot be cast to a float
     * @throws NumberFormatException if the object is a String that cannot be parsed as a float
     */
    public static float object2Float(Object v) {
        return (v instanceof String) ? Float.parseFloat((String) v) : (float) v;
    }

    /**
     * Transforms the given object to a boolean value.
     * If the object is a string, it parses the string as a boolean.
     * Otherwise, it assumes the object is a boolean.
     *
     * @param v the object to be transformed
     * @return the boolean representation of the provided object
     */
    public static boolean object2Boolean(Object v) {
        return (v instanceof String) ? Boolean.parseBoolean((String) v) : (boolean) v;
    }
}
