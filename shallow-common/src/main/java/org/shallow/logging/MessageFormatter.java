package org.shallow.logging;

import java.util.HashSet;
import java.util.Set;

public final class MessageFormatter {
    private static final String DELIM_STR = "{}";
    private static final char ESCAPE_CHAR = '\\';

    static FormattingTuple format(String messagePattern, Object arg) {
        return arrayFormat(messagePattern, new Object[]{arg});
    }

    static FormattingTuple format(final String messagePattern,
                                  Object argA, Object argB) {
        return arrayFormat(messagePattern, new Object[]{argA, argB});
    }

    static FormattingTuple arrayFormat(final String messagePattern,
                                       final Object[] argArray) {
        if (argArray == null || argArray.length == 0) {
            return new FormattingTuple(messagePattern, null);
        }

        int lastArrIdx = argArray.length - 1;
        Object lastEntry = argArray[lastArrIdx];
        Throwable throwable = lastEntry instanceof Throwable? (Throwable) lastEntry : null;

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
        return new FormattingTuple(sb.toString(), L <= lastArrIdx? throwable : null);
    }

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

    private static void safeObjectAppend(StringBuilder sb, Object o) {
        try {
            String oAsString = o.toString();
            sb.append(oAsString);
        } catch (Throwable t) {
            System.err.println("SLF4J: Failed toString() invocation on an object of type [" + o.getClass().getName() + ']');
            t.printStackTrace();
            sb.append("[FAILED toString()]");
        }
    }

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

    private MessageFormatter() {
    }
}
