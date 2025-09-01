package org.ephemq.common.util;

import java.util.Collection;
import java.util.Map;
import javax.naming.OperationNotSupportedException;

/**
 * Utility class providing various object and parameter validation methods.
 * This is a final class with a private constructor to prevent instantiation.
 */
public final class ObjectUtil {

    /**
     * A constant holding the value of zero in float data type.
     * This constant can be used where a floating-point zero is needed and
     * helps achieve consistency and readability in the code.
     */
    private static final float FLOAT_ZERO = 0.0F;
    /**
     * A constant representing the double value zero (0.0).
     * This value can be used as a default or initial value for
     * double variables to ensure a consistent starting point.
     */
    private static final double DOUBLE_ZERO = 0.0D;
    /**
     * A constant representing the long value zero (`0L`).
     * This constant can be used wherever a long representation of zero is required,
     * providing better readability and avoiding magic numbers in the codebase.
     */
    private static final long LONG_ZERO = 0L;
    /**
     * A constant representing the integer value zero.
     * This constant can be used in calculations or comparisons where a zero value is required.
     */
    private static final int INT_ZERO = 0;

    /**
     * The ObjectUtil class constructor is private to prevent instantiation.
     * This utility class is not meant to be instantiated and will throw an
     * OperationNotSupportedException if attempted.
     *
     * @throws OperationNotSupportedException always thrown to indicate that
     *                                        the instantiation of this utility class is not supported.
     */
    private ObjectUtil() throws OperationNotSupportedException {
        throw new OperationNotSupportedException();
    }

    /**
     * Ensures that the provided argument is not null.
     *
     * @param <T> the type of the input argument
     * @param arg the argument to check for nullity
     * @param text the detail message to be used in the event that a {@code NullPointerException} is thrown
     * @return the non-null argument that was validated
     * @throws NullPointerException if {@code arg} is null
     */
    public static <T> T checkNotNull(T arg, String text) {
        if (arg == null) {
            throw new NullPointerException(text);
        }
        return arg;
    }

    /**
     * Checks all provided varargs elements for null and throws a {@link NullPointerException} if any are null.
     *
     * @param text The message to be used in the exception if any element is null.
     * @param varargs The array of elements to be checked for null.
     * @return The varargs array if none of the elements are null.
     * @throws NullPointerException if any of the elements or the entire array itself is null.
     */
    @SafeVarargs
    public static <T> T[] deepCheckNotNull(String text, T... varargs) {
        if (varargs == null) {
            throw new NullPointerException(text);
        }

        for (T element : varargs) {
            if (element == null) {
                throw new NullPointerException(text);
            }
        }
        return varargs;
    }

    /**
     *
     */
    public static <T> T checkNotNullWithIAE(final T arg, final String paramName) throws IllegalArgumentException {
        if (arg == null) {
            throw new IllegalArgumentException(String.format("Param '%s' must not be null", paramName));
        }
        return arg;
    }

    /**
     * Checks if an element of an array is not null, throwing an IllegalArgumentException if it is.
     *
     * @param <T> the type of the array element
     * @param value the element of the array to be checked
     * @param index the index of the element in the array
     * @param name the name of the parameter being checked
     * @return the checked value if it is not null
     * @throws IllegalArgumentException if the value is null
     */
    public static <T> T checkNotNullArrayParam(T value, int index, String name) throws IllegalArgumentException {
        if (value == null) {
            throw new IllegalArgumentException(String.format("Array index[%s] of parameter '%s' must not be null", index, name));
        }
        return value;
    }

    /**
     *
     */
    public static int checkPositive(int i, String name) {
        if (i <= INT_ZERO) {

            throw new IllegalArgumentException(String.format("%s : %s (expected: > 0", name, i));
        }
        return i;
    }

    /**
     * Checks whether the provided integer is negative.
     *
     * @param i the integer to check
     * @param name the name of the parameter, for use in the error message if the check fails
     * @return the provided integer if it is negative
     * @throws IllegalArgumentException if the provided integer is not negative
     */
    public static int checkNegative(int i, String name) {
        if (i >= INT_ZERO) {
            throw new IllegalArgumentException(String.format("%s : %s (expected: < 0", name, i));
        }
        return i;
    }


    /**
     * Checks if the provided long value is positive.
     *
     * @param l the long value to be checked
     * @param name the name of the parameter to include in the exception message if validation fails
     * @return the original long value if it is positive
     * @throws IllegalArgumentException if the provided long value is not positive
     */
    public static long checkPositive(long l, String name) {
        if (l <= LONG_ZERO) {
            throw new IllegalArgumentException(String.format("%s : %s (expected: > 0", name, l));
        }
        return l;
    }

    /**
     * Checks if the provided double value is positive.
     *
     * @param d the double value to check
     * @param name the name of the parameter for error message reporting
     * @return the positive double value if the check passes
     * @throws IllegalArgumentException if the double value is not positive
     */
    public static double checkPositive(final double d, final String name) {
        if (d <= DOUBLE_ZERO) {
            throw new IllegalArgumentException(String.format("%s : %s (expected: > 0", name, d));
        }
        return d;
    }

    /**
     * Checks if the specified float value is positive.
     *
     * @param f the float value to check
     * @param name the name of the parameter to be used in the exception message
     * @return the original float value if it is positive
     * @throws IllegalArgumentException if the specified float value is not positive
     */
    public static float checkPositive(final float f, final String name) {
        if (f <= FLOAT_ZERO) {
            throw new IllegalArgumentException(String.format("%s : %s (expected: > 0", name, f));
        }
        return f;
    }

    /**
     * Checks that the given integer is positive or zero.
     *
     * @param i the integer to check
     * @param name the name of the parameter being checked
     * @return the validated integer if it is positive or zero
     * @throws IllegalArgumentException if the integer is negative
     */
    public static int checkPositiveOrZero(int i, String name) {
        if (i < INT_ZERO) {
            throw new IllegalArgumentException(String.format("%s : %s (expected: >= 0", name, i));
        }
        return i;
    }

    /**
     * Checks if the given long value is positive or zero.
     * If the value is negative, an IllegalArgumentException is thrown.
     *
     * @param l the long value to check
     * @param name the name of the argument to include in the exception message
     * @return the validated long value if it is positive or zero
     * @throws IllegalArgumentException if the value is negative
     */
    public static long checkPositiveOrZero(long l, String name) {
        if (l < LONG_ZERO) {
            throw new IllegalArgumentException(String.format("%s : %s (expected: >= 0", name, l));
        }
        return l;
    }

    /**
     * Checks if the given double value is positive or zero.
     *
     * @param d the double value to be checked
     * @param name the name of the parameter being checked, used in the exception message if the value is invalid
     * @return the validated double value if it is positive or zero
     * @throws IllegalArgumentException if the value is negative
     */
    public static double checkPositiveOrZero(final double d, final String name) {
        if (d < DOUBLE_ZERO) {
            throw new IllegalArgumentException(String.format("%s : %s (expected: >= 0", name, d));
        }
        return d;
    }

    /**
     * Ensures that the specified floating-point number is non-negative.
     *
     * @param f The floating-point number to check.
     * @param name The name of the parameter to include in the exception message if the validation fails.
     * @return The validated floating-point number if it is non-negative.
     * @throws IllegalArgumentException if the specified floating-point number is negative.
     */
    public static float checkPositiveOrZero(final float f, final String name) {
        if (f < FLOAT_ZERO) {
            throw new IllegalArgumentException(String.format("%s : %s (expected: >= 0", name, f));
        }
        return f;
    }

    /**
     * Checks if the provided integer is within the specified range.
     *
     * @param i the integer value to check.
     * @param start the start of the range (inclusive).
     * @param end the end of the range (inclusive).
     * @param name the name of the parameter for error messaging.
     * @return the input integer if it is within the specified range.
     * @throws IllegalArgumentException if the integer is not within the specified range.
     */
    public static int checkInRange(int i, int start, int end, String name) {
        if (i < start || i > end) {
            throw new IllegalArgumentException(String.format("%s:%s (expected: %s - %s)", name, i, start, end));

        }
        return i;
    }

    /**
     * Checks if a given long value is within the specified range [start, end].
     *
     * @param l the long value to check
     * @param start the lower bound of the range (inclusive)
     * @param end the upper bound of the range (inclusive)
     * @param name the name of the parameter, used in the exception message
     * @return the value of l if it is within the specified range
     * @throws IllegalArgumentException if the value is not within the specified range
     */
    public static long checkInRange(long l, long start, long end, String name) {
        if (l < start || l > end) {
            throw new IllegalArgumentException(String.format("%s:%s (expected: %s - %s)", name, l, start, end));
        }
        return l;
    }

    /**
     * Checks if the provided array is not empty.
     *
     * @param array the array to be checked for non-emptiness
     * @param name the name of the array parameter being checked, used in exception message if validation fails
     * @return the input array if it is not empty
     * @throws IllegalArgumentException if the array is empty
     */
    public static <T> T[] checkNonEmpty(T[] array, String name) {
        //No String concatenation for check
        if (checkNotNull(array, name).length == 0) {
            throw new IllegalArgumentException(String.format("Param[%s] must not be empty", name));
        }
        return array;
    }

    /**
     *
     */
    public static byte[] checkNonEmpty(byte[] array, String name) {
        //No String concatenation for check
        if (checkNotNull(array, name).length == 0) {
            throw new IllegalArgumentException(String.format("Param[%s] must not be empty", name));
        }
        return array;
    }

    /**
     * Checks if the provided char array is non-null and non-empty.
     *
     * @param array the char array to check
     * @param name the name of the parameter, used in exception messages
     * @return the input char array if it is non-null and non-empty
     * @throws IllegalArgumentException if the input char array is empty
     * @throws NullPointerException if the input char array is null
     */
    public static char[] checkNonEmpty(char[] array, String name) {
        //No String concatenation for check
        if (checkNotNull(array, name).length == 0) {
            throw new IllegalArgumentException(String.format("Param[%s] must not be empty", name));
        }
        return array;
    }

    /**
     * Checks that the provided collection is not null and not empty.
     *
     * @param collection the collection to check
     * @param name the name of the collection parameter
     * @return the provided collection if it is not null and not empty
     * @throws IllegalArgumentException if the collection is empty
     * @throws NullPointerException if the collection is null
     */
    public static <T extends Collection<?>> T checkNonEmpty(T collection, String name) {
        if (checkNotNull(collection, name).isEmpty()) {
            throw new IllegalArgumentException(String.format("Param[%s] must not be empty", name));
        }
        return collection;
    }

    /**
     * Validates that the provided string value is not null and not empty.
     *
     * @param value the string to check
     * @param name the name of the parameter to include in the exception message
     * @return the input value if it is not null and not empty
     * @throws IllegalArgumentException if the input value is empty
     */
    public static String checkNonEmpty(final String value, final String name) {
        if (checkNotNull(value, name).isEmpty()) {
            throw new IllegalArgumentException(String.format("Param[%s] must not be empty", name));
        }
        return value;
    }

    /**
     * Ensures that the provided Map is neither null nor empty.
     *
     * @param value the Map to be checked
     * @param name the name of the parameter to be included in the exception message
     * @param <K> the type of keys maintained by the map
     * @param <V> the type of mapped values
     * @param <T> the type of the map
     * @return the validated Map
     * @throws IllegalArgumentException if the provided Map is empty
     * @throws NullPointerException if the provided Map is null
     */
    public static <K, V, T extends Map<K, V>> T checkNonEmpty(T value, String name) {
        if (checkNotNull(value, name).isEmpty()) {
            throw new IllegalArgumentException(String.format("Param[%s] must not be empty", name));
        }
        return value;
    }

    /**
     * Verifies that the given CharSequence is not null and not empty.
     *
     * @param value the CharSequence to check
     * @param name the name of the parameter, used in the exception message if the check fails
     * @return the original CharSequence if it is not null and not empty
     * @throws NullPointerException if the value is null
     * @throws IllegalArgumentException if the value is empty
     */
    public static CharSequence checkNonEmpty(final CharSequence value, final String name) {
        if (checkNotNull(value, name).isEmpty()) {
            throw new IllegalArgumentException(String.format("Param[%s] must not be empty", name));
        }
        return value;
    }

    /**
     * Validates that the given string is not null and not empty after trimming.
     *
     * @param value The string to be checked.
     * @param name The name that represents the string for error messaging.
     * @return The trimmed string if it is neither null nor empty.
     */
    public static String checkNonEmptyAfterTrim(final String value, final String name) {
        String trimmed = checkNotNull(value, name).trim();
        return checkNonEmpty(trimmed, name);
    }

    /**
     * Returns the int value of the Integer object if it is not null,
     * otherwise returns the default value provided.
     *
     * @param wrapper the Integer object which may be null
     * @param defaultValue the default value to return if wrapper is null
     * @return the int value of the wrapper or defaultValue if wrapper is null
     */
    public static int intValue(Integer wrapper, int defaultValue) {
        return wrapper != null ? wrapper : defaultValue;
    }

    /**
     * Returns the value of the specified {@code wrapper} if it is non-null,
     * otherwise returns the {@code defaultValue}.
     *
     * @param wrapper the {@code Long} object to unwrap, may be {@code null}
     * @param defaultValue the default value to return if {@code wrapper} is {@code null}
     * @return the unwrapped value of {@code wrapper} if it is non-null, otherwise {@code defaultValue}
     */
    public static long longValue(Long wrapper, long defaultValue) {
        return wrapper != null ? wrapper : defaultValue;
    }
}
