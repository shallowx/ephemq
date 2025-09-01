package org.ephemq.common.util;

import javax.naming.OperationNotSupportedException;
import java.util.Arrays;

/**
 * Utility class for various String-related operations.
 */
public final class StringUtil {

    /**
     * A constant representing the double-quote character (").
     * This is commonly used for tasks such as wrapping text in double-quote characters
     * or escaping double quotes in string literals.
     */
    public static final char DOUBLE_QUOTE = '\"';
    /**
     * Represents the tab character ('\t').
     * This constant can be used wherever a tab character is needed
     * in string manipulations or formatting.
     */
    public static final char TAB = '\t';
    /**
     * Represents the space character with the UTF-16 code value of 0x20.
     * It is used for handling operations that require a space character.
     */
    public static final char SPACE = 0x20;

    /**
     * A precomputed lookup table to convert a byte value to a two-character hexadecimal string with padding.
     * The index into the array corresponds to the byte value (0-255).
     * This table is utilized for efficient byte-to-hexadecimal string conversion.
     */
    private static final String[] BYTE2HEX_PAD = new String[256];
    /**
     * A lookup table used to convert byte values to their hexadecimal string representation without padding.
     * Each entry in the array corresponds to the hex string representation of the byte value at the same index.
     * This can be used to efficiently convert bytes to hex strings where padding (e.g., leading zeros) is not required.
     */
    private static final String[] BYTE2HEX_NOPAD = new String[256];
    /**
     * A static final byte array utilized for hexadecimal conversions within the StringUtil class.
     * This array is designed to map hexadecimal digit characters to their corresponding byte values.
     */
    private static final byte[] HEX2B;

    /**
     * This character is used to separate the components of a package name in a fully qualified class name.
     * It is defined as a period ('.'), which is the standard Java package separator.
     */
    private static final char PACKAGE_SEPARATOR_CHAR = '.';

    static {
        for (int i = 0; i < BYTE2HEX_PAD.length; i++) {
            String str = Integer.toHexString(i);
            BYTE2HEX_PAD[i] = i > 0xf ? str : ('0' + str);
            BYTE2HEX_NOPAD[i] = str;
        }

        HEX2B = new byte[Character.MAX_VALUE + 1];
        Arrays.fill(HEX2B, (byte) -1);
        HEX2B['0'] = (byte) 0;
        HEX2B['1'] = (byte) 1;
        HEX2B['2'] = (byte) 2;
        HEX2B['3'] = (byte) 3;
        HEX2B['4'] = (byte) 4;
        HEX2B['5'] = (byte) 5;
        HEX2B['6'] = (byte) 6;
        HEX2B['7'] = (byte) 7;
        HEX2B['8'] = (byte) 8;
        HEX2B['9'] = (byte) 9;
        HEX2B['A'] = (byte) 10;
        HEX2B['B'] = (byte) 11;
        HEX2B['C'] = (byte) 12;
        HEX2B['D'] = (byte) 13;
        HEX2B['E'] = (byte) 14;
        HEX2B['F'] = (byte) 15;
        HEX2B['a'] = (byte) 10;
        HEX2B['b'] = (byte) 11;
        HEX2B['c'] = (byte) 12;
        HEX2B['d'] = (byte) 13;
        HEX2B['e'] = (byte) 14;
        HEX2B['f'] = (byte) 15;
    }

    /**
     * Private constructor to prevent instantiation of the StringUtil class.
     * <p>
     * This constructor throws an OperationNotSupportedException to ensure that
     * the StringUtil class cannot be instantiated, as it is designed to be a
     * utility class with static methods only.
     *
     * @throws OperationNotSupportedException always thrown to prevent instantiation
     */
    private StringUtil() throws OperationNotSupportedException {
        // Unused.
        throw new OperationNotSupportedException();
    }

    /**
     * Returns the simple class name of the provided object.
     *
     * @param o the object whose simple class name is to be determined; if null, "null_object" is returned.
     * @return the simple class name of the object's class, or "null_object" if the provided object is null.
     */
    public static String simpleClassName(Object o) {
        if (o == null) {
            return "null_object";
        } else {
            return simpleClassName(o.getClass());
        }
    }

    /**
     * Returns the simple name of the class represented by the specified {@code clazz}.
     *
     * @param clazz the class whose simple name is to be determined
     * @return the simple name of the class represented by {@code clazz}
     */
    public static String simpleClassName(Class<?> clazz) {
        String className = ObjectUtil.checkNotNull(clazz, "clazz").getName();
        final int lastDotIdx = className.lastIndexOf(PACKAGE_SEPARATOR_CHAR);
        if (lastDotIdx > -1) {
            return className.substring(lastDotIdx + 1);
        }
        return className;
    }

    /**
     * Returns the length of the given string.
     * If the string is null, it returns 0.
     *
     * @param s the string whose length is to be calculated
     * @return the length of the string, or 0 if the string is null
     */
    public static int length(String s) {
        return s == null ? 0 : s.length();
    }

    /**
     * Checks if a given string is null or empty.
     *
     * @param s the string to check
     * @return true if the string is null or empty, false otherwise
     */
    public static boolean isNullOrEmpty(String s) {
        return s == null || s.isEmpty();
    }

    /**
     * Checks if the given character is a double quote.
     *
     * @param c the character to check
     * @return true if the character is a double quote, false otherwise
     */
    private static boolean isDoubleQuote(char c) {
        return c == DOUBLE_QUOTE;
    }

    /**
     * Finds the index of the first character that is not an OWS (optional whitespace) character
     * in the given CharSequence.
     *
     * @param value the CharSequence to be examined
     * @param length the length of the CharSequence
     * @return the index of the first non-OWS character, or the length of the CharSequence if all characters are OWS
     */
    private static int indexOfFirstNonOwsChar(CharSequence value, int length) {
        int i = 0;
        while (i < length && isOws(value.charAt(i))) {
            i++;
        }
        return i;
    }

    /**
     * Returns the index of the last non-OWS (optional whitespace) character in the given CharSequence.
     *
     * @param value the CharSequence to check
     * @param start the starting index to check from
     * @param length the length of the CharSequence
     * @return the index of the last non-OWS character, or -1 if no such character is found
     */
    private static int indexOfLastNonOwsChar(CharSequence value, int start, int length) {
        int i = length - 1;
        while (i > start && isOws(value.charAt(i))) {
            i--;
        }
        return i;
    }

    /**
     * Checks if the given character is a whitespace character.
     * The method considers a character to be whitespace if it is either a space or a tab.
     *
     * @param c the character to check
     * @return true if the character is a space or a tab, false otherwise
     */
    private static boolean isOws(char c) {
        return c == SPACE || c == TAB;
    }

}
