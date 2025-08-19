package org.meteor.common.logging;

import java.util.HashSet;
import java.util.Set;

/**
 * MessageFormatter is a utility class that provides methods for formatting messages
 * with placeholder substitution. It supports formatting messages with one or more
 * arguments and handles special cases such as escaped placeholders and throwable objects.
 */
public final class MessageFormatter {
    /**
     * The delimiter string used for argument substitution in message formatting.
     * This string "{}" acts as a placeholder for message arguments in formatted log messages.
     */
    private static final String DELIM_STR = "{}";
    /**
     * The escape character used in message formatting.
     * This character is used to escape other characters,
     * enabling the inclusion of special characters in formatted strings.
     */
    private static final char ESCAPE_CHAR = '\\';

    /**
     * Private constructor to prevent instantiation of the MessageFormatter utility class.
     * This class provides static methods for formatting messages.
     */
    private MessageFormatter() {
    }

    /**
     * Formats a message with a single argument.
     *
     * @param messagePattern the string pattern containing placeholders
     * @param arg            the argument to be substituted into the message pattern
     * @return a FormattingTuple containing the formatted message and any associated throwable
     */
    static FormattingTuple format(String messagePattern, Object arg) {
        return arrayFormat(messagePattern, new Object[]{arg});
    }

    /**
     * Formats a message using the specified message pattern and two arguments.
     *
     * @param messagePattern the pattern to be used for formatting the message
     * @param argA the first argument to be used in the message pattern
     * @param argB the second argument to be used in the message pattern
     * @return a FormattingTuple containing the formatted message and any associated throwable
     */
    static FormattingTuple format(final String messagePattern,
                                  Object argA, Object argB) {
        return arrayFormat(messagePattern, new Object[]{argA, argB});
    }

    /**
     * Formats a message with the given pattern and an array of arguments.
     * This method substitutes each placeholder in the message pattern with
     * the corresponding argument from the array.
     *
     * @param messagePattern the message pattern containing placeholders delimited by `{}`.
     * @param argArray the array of arguments to be substituted into the message pattern.
     * @return a {@link FormattingTuple} containing the formatted message and any associated throwable from the argument array.
     */
    static FormattingTuple arrayFormat(final String messagePattern,
                                       final Object[] argArray) {
        if (argArray == null || argArray.length == 0) {
            return new FormattingTuple(messagePattern, null);
        }

        int lastArrIdx = argArray.length - 1;
        Object lastEntry = argArray[lastArrIdx];
        Throwable throwable = lastEntry instanceof Throwable ? (Throwable) lastEntry : null;

        if (messagePattern == null) {
            return new FormattingTuple(null, throwable);
        }

        int j = messagePattern.indexOf(DELIM_STR);
        if (j == -1) {
            return new FormattingTuple(messagePattern, throwable);
        }

        StringBuilder sb = new StringBuilder(messagePattern.length() + 50);
        int i = 0;
        int L = 0;
        do {
            boolean notEscaped = j == 0 || messagePattern.charAt(j - 1) != ESCAPE_CHAR;
            if (notEscaped) {
                sb.append(messagePattern, i, j);
            } else {
                sb.append(messagePattern, i, j - 1);
                notEscaped = j >= 2 && messagePattern.charAt(j - 2) == ESCAPE_CHAR;
            }

            i = j + 2;
            if (notEscaped) {
                deeplyAppendParameter(sb, argArray[L], null);
                L++;
                if (L > lastArrIdx) {
                    break;
                }
            } else {
                sb.append(DELIM_STR);
            }
            j = messagePattern.indexOf(DELIM_STR, i);
        } while (j != -1);

        sb.append(messagePattern, i, messagePattern.length());
        return new FormattingTuple(sb.toString(), L <= lastArrIdx ? throwable : null);
    }

    /**
     * Recursively appends a parameter to the given StringBuilder, handling arrays and
     * preventing infinite loops with circular references.
     *
     * @param sb the StringBuilder to append to
     * @param o the object to append, which can be of any type including arrays
     * @param seenSet a Set of Object arrays used to detect and prevent circular references
     */
    private static void deeplyAppendParameter(StringBuilder sb, Object o,
                                              Set<Object[]> seenSet) {
        if (o == null) {
            sb.append("null");
            return;
        }
        Class<?> objClass = o.getClass();
        if (!objClass.isArray()) {
            if (Number.class.isAssignableFrom(objClass)) {
                if (objClass == Long.class) {
                    sb.append(((Long) o).longValue());
                } else if (objClass == Integer.class || objClass == Short.class || objClass == Byte.class) {
                    sb.append(((Number) o).intValue());
                } else if (objClass == Double.class) {
                    sb.append(((Double) o).doubleValue());
                } else if (objClass == Float.class) {
                    sb.append(((Float) o).floatValue());
                } else {
                    safeObjectAppend(sb, o);
                }
            } else {
                safeObjectAppend(sb, o);
            }
        } else {
            sb.append('[');
            if (objClass == boolean[].class) {
                booleanArrayAppend(sb, (boolean[]) o);
            } else if (objClass == byte[].class) {
                byteArrayAppend(sb, (byte[]) o);
            } else if (objClass == char[].class) {
                charArrayAppend(sb, (char[]) o);
            } else if (objClass == short[].class) {
                shortArrayAppend(sb, (short[]) o);
            } else if (objClass == int[].class) {
                intArrayAppend(sb, (int[]) o);
            } else if (objClass == long[].class) {
                longArrayAppend(sb, (long[]) o);
            } else if (objClass == float[].class) {
                floatArrayAppend(sb, (float[]) o);
            } else if (objClass == double[].class) {
                doubleArrayAppend(sb, (double[]) o);
            } else {
                objectArrayAppend(sb, (Object[]) o, seenSet);
            }
            sb.append(']');
        }
    }

    /**
     * Safely appends the string representation of an object to a StringBuilder.
     * If the object's toString method throws an exception, an error message
     * and stack trace are printed to the standard error stream, and a fallback
     * string "[FAILED toString()]" is appended instead.
     *
     * @param sb the StringBuilder to append to
     * @param o  the object whose string representation is to be appended
     */
    private static void safeObjectAppend(StringBuilder sb, Object o) {
        try {
            String oAsString = o.toString();
            sb.append(oAsString);
        } catch (Throwable t) {
            System.err.printf("SLF4J: Failed toString() invocation on an object of type [%s]%n", o.getClass().getName());
            t.printStackTrace();
            sb.append("[FAILED toString()]");
        }
    }

    /**
     * Appends the string representation of an object array to the provided StringBuilder.
     * It ensures that arrays already seen are not appended again to avoid cyclic references.
     *
     * @param sb the StringBuilder to which the array's string representation is appended
     * @param a the array of objects to be appended
     * @param seenSet a Set used to track arrays that have already been appended to avoid cyclic references
     */
    private static void objectArrayAppend(StringBuilder sb, Object[] a, Set<Object[]> seenSet) {
        if (a.length == 0) {
            return;
        }
        if (seenSet == null) {
            seenSet = new HashSet<>(a.length);
        }
        if (seenSet.add(a)) {
            deeplyAppendParameter(sb, a[0], seenSet);
            for (int i = 1; i < a.length; i++) {
                sb.append(", ");
                deeplyAppendParameter(sb, a[i], seenSet);
            }
            seenSet.remove(a);
        } else {
            sb.append("...");
        }
    }

    /**
     * Appends the string representation of a boolean array to the provided StringBuilder.
     *
     * @param sb the StringBuilder to which the boolean array will be appended
     * @param a the boolean array whose string representation will be appended
     */
    private static void booleanArrayAppend(StringBuilder sb, boolean[] a) {
        if (a.length == 0) {
            return;
        }
        sb.append(a[0]);
        for (int i = 1; i < a.length; i++) {
            sb.append(", ");
            sb.append(a[i]);
        }
    }

    /**
     * Appends the contents of a byte array to a given StringBuilder, separating each byte with a comma.
     *
     * @param sb the StringBuilder to append to
     * @param a the byte array whose contents are to be appended
     */
    private static void byteArrayAppend(StringBuilder sb, byte[] a) {
        if (a.length == 0) {
            return;
        }
        sb.append(a[0]);
        for (int i = 1; i < a.length; i++) {
            sb.append(", ");
            sb.append(a[i]);
        }
    }

    /**
     * Appends the contents of a char array to a StringBuilder, separating each element with ", ".
     *
     * @param sb the StringBuilder to append to
     * @param a the char array whose contents are to be appended
     */
    private static void charArrayAppend(StringBuilder sb, char[] a) {
        if (a.length == 0) {
            return;
        }
        sb.append(a[0]);
        for (int i = 1; i < a.length; i++) {
            sb.append(", ");
            sb.append(a[i]);
        }
    }

    /**
     * Appends the string representation of the short array to the given StringBuilder,
     * with each element separated by a comma and a space.
     *
     * @param sb the StringBuilder to append the array elements to
     * @param a the short array whose elements are to be appended
     */
    private static void shortArrayAppend(StringBuilder sb, short[] a) {
        if (a.length == 0) {
            return;
        }
        sb.append(a[0]);
        for (int i = 1; i < a.length; i++) {
            sb.append(", ");
            sb.append(a[i]);
        }
    }

    /**
     * Appends the string representations of the elements of an int array to a StringBuilder,
     * separating each element with a comma and a space.
     *
     * @param sb the StringBuilder to append to
     * @param a the int array whose elements are to be appended
     */
    private static void intArrayAppend(StringBuilder sb, int[] a) {
        if (a.length == 0) {
            return;
        }
        sb.append(a[0]);
        for (int i = 1; i < a.length; i++) {
            sb.append(", ");
            sb.append(a[i]);
        }
    }

    /**
     * Appends the elements of a long array to a StringBuilder, separated by commas.
     *
     * @param sb the StringBuilder to append the elements to
     * @param a the long array whose elements are to be appended
     */
    private static void longArrayAppend(StringBuilder sb, long[] a) {
        if (a.length == 0) {
            return;
        }
        sb.append(a[0]);
        for (int i = 1; i < a.length; i++) {
            sb.append(", ");
            sb.append(a[i]);
        }
    }

    /**
     * Appends the elements of a float array to a StringBuilder as a comma-separated list.
     *
     * @param sb the StringBuilder to which the float array elements will be appended
     * @param a the float array whose elements are to be appended
     */
    private static void floatArrayAppend(StringBuilder sb, float[] a) {
        if (a.length == 0) {
            return;
        }
        sb.append(a[0]);
        for (int i = 1; i < a.length; i++) {
            sb.append(", ");
            sb.append(a[i]);
        }
    }

    /**
     * Appends the contents of a double array to a StringBuilder, separating values with commas.
     *
     * @param sb the StringBuilder to which the double array will be appended
     * @param a the double array whose elements are to be appended to the StringBuilder
     */
    private static void doubleArrayAppend(StringBuilder sb, double[] a) {
        if (a.length == 0) {
            return;
        }
        sb.append(a[0]);
        for (int i = 1; i < a.length; i++) {
            sb.append(", ");
            sb.append(a[i]);
        }
    }
}
