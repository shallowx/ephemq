package org.ephemq.common.util;

import io.netty.util.internal.EmptyArrays;
import io.netty.util.internal.StringUtil;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

import java.util.Arrays;
import java.util.Collections;

import static io.netty.util.internal.StringUtil.*;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.*;

public class StringUtilTest {

    /**
     * Asserts that the strings do not have a common suffix of the given length.
     *
     * @param s   the first string to test
     * @param p   the second string to test
     * @param len the length of the suffix to check for commonality
     */
    private static void checkNotCommonSuffix(String s, String p, int len) {
        assertFalse(checkCommonSuffixSymmetric(s, p, len));
    }

    /**
     * Asserts that two strings share a common suffix of the specified length.
     *
     * @param s the first string to be compared
     * @param p the second string to be compared
     * @param len the length of the suffix to check for
     */
    private static void checkCommonSuffix(String s, String p, int len) {
        assertTrue(checkCommonSuffixSymmetric(s, p, len));
    }

    /**
     * Checks if two strings have a common suffix of a given length, and verifies that the result is symmetric.
     * This means that if one string has a common suffix with the other, the reverse should be true as well.
     *
     * @param s the first string to compare
     * @param p the second string to compare
     * @param len the length of the suffix to check for commonality
     * @return true if both strings share a common suffix of the specified length, false otherwise
     */
    private static boolean checkCommonSuffixSymmetric(String s, String p, int len) {
        boolean sp = commonSuffixOfLength(s, p, len);
        boolean ps = commonSuffixOfLength(p, s, len);
        assertEquals(sp, ps);
        return sp;
    }

    /**
     * Escapes a given CharSequence value for CSV format and compares it with an expected result.
     *
     * @param value the input CharSequence to be escaped for CSV
     * @param expected the expected escaped CharSequence result
     */
    private static void escapeCsv(CharSequence value, CharSequence expected) {
        escapeCsv(value, expected, false);
    }

    /**
     * Escapes the given CSV value while trimming any outer white spaces and comparing with the expected value.
     *
     * @param value The original CSV value to be escaped and trimmed.
     * @param expected The expected result after escaping and trimming the CSV value.
     */
    private static void escapeCsvWithTrimming(CharSequence value, CharSequence expected) {
        escapeCsv(value, expected, true);
    }

    /**
     * Escapes a CSV value and verifies the result against the expected output.
     *
     * @param value the CSV value to be escaped
     * @param expected the expected escaped CSV value
     * @param trimOws a flag indicating whether to trim optional whitespaces around the CSV elements
     */
    private static void escapeCsv(CharSequence value, CharSequence expected, boolean trimOws) {
        CharSequence escapedValue = value;
        for (int i = 0; i < 10; ++i) {
            escapedValue = StringUtil.escapeCsv(escapedValue, trimOws);
            assertEquals(expected, escapedValue.toString());
        }
    }

    /**
     * Asserts that the CSV escape and unescape operations on the given value result in the original value.
     *
     * @param value the input string to be tested for escaping and unescaping via CSV rules
     */
    private static void assertEscapeCsvAndUnEscapeCsv(String value) {
        assertEquals(value, unescapeCsv(StringUtil.escapeCsv(value)));
    }

    /**
     * Tests the simple class name generation by comparing it with the expected simple class name.
     *
     * @param clazz the Class object being tested for its simple name
     * @throws Exception if there is an error during the test execution
     */
    private static void testSimpleClassName(Class<?> clazz) throws Exception {
        Package pkg = clazz.getPackage();
        String name;
        if (pkg != null) {
            name = clazz.getName().substring(pkg.getName().length() + 1);
        } else {
            name = clazz.getName();
        }
        assertEquals(name, simpleClassName(clazz));
    }

    /**
     * Ensures that the `NEWLINE` constant is not null.
     * <p>
     * This test verifies that the `NEWLINE` field is initialized and holds a non-null value.
     * The existence of a newline constant is essential for various string manipulations
     * and this test confirms its presence in the context where it's expected to be used.
     * <p>
     * The test will fail if `NEWLINE` is null, indicating that the required constant is not properly set.
     */
    @Test
    public void ensureNewlineExists() {
        assertNotNull(NEWLINE);
    }

    /**
     * Tests the toHexString method with various byte array inputs.
     * Verifies that the output matches the expected hexadecimal string representations.
     */
    @Test
    public void testToHexString() {
        assertThat(toHexString(new byte[]{0}), is("0"));
        assertThat(toHexString(new byte[]{1}), is("1"));
        assertThat(toHexString(new byte[]{0, 0}), is("0"));
        assertThat(toHexString(new byte[]{1, 0}), is("100"));
        assertThat(toHexString(EmptyArrays.EMPTY_BYTES), is(""));
    }

    /**
     * Test method for verifying the correct behavior of the toHexStringPadded method.
     * <p>
     * This method checks the padded hexadecimal string conversion of various byte arrays.
     * It includes tests for the following cases:
     * <ul>
     *   <li>Single zero byte</li>
     *   <li>Single byte with value 1</li>
     *   <li>Two zero bytes</li>
     *   <li>Byte array with values 1 and 0</li>
     *   <li>Empty byte array</li>
     * </ul>
     */
    @Test
    public void testToHexStringPadded() {
        assertThat(toHexStringPadded(new byte[]{0}), is("00"));
        assertThat(toHexStringPadded(new byte[]{1}), is("01"));
        assertThat(toHexStringPadded(new byte[]{0, 0}), is("0000"));
        assertThat(toHexStringPadded(new byte[]{1, 0}), is("0100"));
        assertThat(toHexStringPadded(EmptyArrays.EMPTY_BYTES), is(""));
    }

    /**
     * Tests the `split` method of the `String` class.
     * <p>
     * The method verifies that splitting the string "foo:bar" using the delimiter ":"
     * results in an array containing two elements: "foo" and "bar".
     */
    @Test
    public void splitSimple() {
        assertArrayEquals(new String[]{"foo", "bar"}, "foo:bar".split(":"));
    }

    /**
     * Tests the splitting of a string with a trailing delimiter.
     * <p>
     * This method uses the `split` function on the string `"foo,bar,"` with `","` as the delimiter.
     * The expected result is an array containing the elements `"foo"` and `"bar"`.
     * The assertion verifies that the result of the split operation equals the expected array.
     */
    @Test
    public void splitWithTrailingDelimiter() {
        assertArrayEquals(new String[]{"foo", "bar"}, "foo,bar,".split(","));
    }

    /**
     * Tests the behavior of the String split method when there are trailing delimiters.
     * This method verifies that splitting a string "foo!bar!!" by the "!" delimiter
     * correctly results in an array with the elements "foo" and "bar".
     */
    @Test
    public void splitWithTrailingDelimiters() {
        assertArrayEquals(new String[]{"foo", "bar"}, "foo!bar!!".split("!"));
    }

    /**
     * Tests the splitting of a string with trailing delimiters using the dot (.) as separator.
     * This test case checks if the trailing delimiters are properly ignored.
     * <p>
     * In this specific test, the input string "foo.bar.." is being split by the dot delimiter
     * resulting in the array ["foo", "bar"].
     */
    @Test
    public void splitWithTrailingDelimitersDot() {
        assertArrayEquals(new String[]{"foo", "bar"}, "foo.bar..".split("\\."));
    }

    /**
     * Tests the behavior of the String.split method when the delimiter is '=' and there are trailing delimiters.
     * Specifically, this ensures the trailing delimiters are handled correctly.
     * The expected result is that trailing empty elements should be omitted.
     */
    @Test
    public void splitWithTrailingDelimitersEq() {
        assertArrayEquals(new String[]{"foo", "bar"}, "foo=bar==".split("="));
    }

    /**
     * Tests the functionality of splitting a string with trailing delimiters.
     * The method verifies that a string with trailing spaces as delimiters
     * is split correctly into an array of strings, ignoring the trailing spaces.
     * <p>
     * The test uses an assert statement to compare the result of the split with the expected output.
     * <p>
     * Specifically, this test checks the behavior of splitting the string "foo bar  "
     * (with two trailing spaces) by the space delimiter, validating that the result is
     * an array containing "foo" and "bar" without any empty strings for the trailing spaces.
     */
    @Test
    public void splitWithTrailingDelimitersSpace() {
        assertArrayEquals(new String[]{"foo", "bar"}, "foo bar  ".split(" "));
    }

    /**
     * Test method to validate the splitting of strings with consecutive delimiters.
     * This method asserts that the string "foo$$bar" when split by the dollar sign ($)
     * results in an array containing "foo", an empty string (representing the consecutive delimiters), and "bar".
     */
    @Test
    public void splitWithConsecutiveDelimiters() {
        assertArrayEquals(new String[]{"foo", "", "bar"}, "foo$$bar".split("\\$"));
    }

    /**
     * Tests the `split` method when the delimiter is at the beginning of the string.
     * This method verifies that the `split` method correctly handles strings with delimiters
     * at the beginning by returning an empty string as the first element in the resulting array.
     */
    @Test
    public void splitWithDelimiterAtBeginning() {
        assertArrayEquals(new String[]{"", "foo", "bar"}, "#foo#bar".split("#"));
    }

    /**
     * Tests the behavior of the `splitMaxPart` method by asserting the results of
     * splitting a string using a delimiter with a limit on the number of parts.
     * <p>
     * This method checks the correctness of string splitting when using a specified
     * limit. It covers two cases:
     * - Splitting the string "foo:bar:bar2" with a limit of 2 parts.
     * - Splitting the string "foo:bar:bar2" with a limit of 3 parts.
     * <p>
     * The assertions verify that:
     * - When the limit is 2, the string is split into two parts: {"foo", "bar:bar2"}.
     * - When the limit is 3, the string is split into three parts: {"foo", "bar", "bar2"}.
     */
    @Test
    public void splitMaxPart() {
        assertArrayEquals(new String[]{"foo", "bar:bar2"}, "foo:bar:bar2".split(":", 2));
        assertArrayEquals(new String[]{"foo", "bar", "bar2"}, "foo:bar:bar2".split(":", 3));
    }

    /**
     * Tests the {@code substringAfter} method to ensure it correctly returns the portion of the
     * input string that follows the first occurrence of the specified delimiter.
     * <p>
     * The test verifies that given the input string "foo:bar:bar2" and the delimiter ':',
     * the method should return "bar:bar2".
     */
    @Test
    public void substringAfterTest() {
        assertEquals("bar:bar2", substringAfter("foo:bar:bar2", ':'));
    }

    /**
     * Tests the functionality of determining common suffixes of a given length
     * between two strings.
     * <p>
     * The method utilizes multiple assertions to validate the behavior of common
     * suffix determination using both valid and invalid input conditions.
     * <p>
     * The cases tested include:
     * - Negative length suffixes, which should never be common.
     * - Handling of null strings, which should not have a suffix.
     * - Validating that any non-null string has a 0-length suffix.
     * - Verifying common suffix of varying lengths between identical strings.
     * - Checking the common suffix between strings with partial matching substrings.
     */
    @Test
    public void commonSuffixOfLengthTest() {
        // negative length suffixes are never common
        checkNotCommonSuffix("abc", "abc", -1);

        // null has no suffix
        checkNotCommonSuffix("abc", null, 0);
        checkNotCommonSuffix(null, null, 0);

        // any non-null string has 0-length suffix
        checkCommonSuffix("abc", "xx", 0);

        checkCommonSuffix("abc", "abc", 0);
        checkCommonSuffix("abc", "abc", 1);
        checkCommonSuffix("abc", "abc", 2);
        checkCommonSuffix("abc", "abc", 3);
        checkNotCommonSuffix("abc", "abc", 4);

        checkCommonSuffix("abcd", "cd", 1);
        checkCommonSuffix("abcd", "cd", 2);
        checkNotCommonSuffix("abcd", "cd", 3);

        checkCommonSuffix("abcd", "axcd", 1);
        checkCommonSuffix("abcd", "axcd", 2);
        checkNotCommonSuffix("abcd", "axcd", 3);

        checkNotCommonSuffix("abcx", "abcy", 1);
    }

    /**
     * Tests the escapeCsv method to ensure it throws a NullPointerException
     * when passed a null value.
     * <p>
     * This test verifies that the method under test correctly handles
     * null inputs by throwing the appropriate exception, which is
     * essential to ensure robustness in handling invalid or unexpected inputs.
     * <p>
     * The method uses assertThrows to specify the expected NullPointerException,
     * ensuring that the escapeCsv method behaves as expected when encountering
     * a null input.
     *
     * @throws NullPointerException if the escapeCsv method is passed a null value
     */
    @Test
    public void escapeCsvNull() {
        assertThrows(NullPointerException.class, new Executable() {
            @Override
            public void execute() {
                StringUtil.escapeCsv(null);
            }
        });
    }

    /**
     * Test method for verifying the behavior of the escapeCsv function with an empty CharSequence.
     * <p>
     * This test ensures that the CSV escaping function correctly handles empty input values.
     * The test asserts that the function's behavior is as expected when provided with an empty input.
     */
    @Test
    public void escapeCsvEmpty() {
        CharSequence value = "";
        escapeCsv(value, value);
    }

    /**
     * Tests the escapeCsv method without quoting the input value.
     * <p>
     * This test verifies that the value "something" is correctly passed
     * and escaped through the escapeCsv method.
     * </p>
     */
    @Test
    public void escapeCsvUnquoted() {
        CharSequence value = "something";
        escapeCsv(value, value);
    }

    /**
     * Tests the CSV escaping functionality for a string that is already quoted.
     * <p>
     * This method validates that an already quoted string is handled correctly by the
     * escapeCsv method, ensuring that it does not alter or double-escape the input.
     * <p>
     * The input value for this test is a quoted string ("something") and the expected
     * output is the same quoted string ("something").
     */
    @Test
    public void escapeCsvAlreadyQuoted() {
        CharSequence value = "\"something\"";
        CharSequence expected = "\"something\"";
        escapeCsv(value, expected);
    }

    /**
     * Tests the escapeCsv method for a CharSequence that contains a quote character.
     * <p>
     * This test verifies the correct escaping of quotes within a CSV string.
     * The input value contains a single quote, and the expected value
     * is the correctly escaped form of the input.
     */
    @Test
    public void escapeCsvWithQuote() {
        CharSequence value = "s\"";
        CharSequence expected = "\"s\"\"\"";
        escapeCsv(value, expected);
    }

    /**
     * Tests the CSV escaping functionality when the provided input contains a quote character in the middle.
     * Specifically, verifies that the input is correctly escaped by doubling the internal quote and enclosing
     * the entire sequence in quotes.
     * <p>
     * The method uses a predefined input sequence `some text"and more text`, and expects it to be escaped as
     * `"some text""and more text"`.
     * <p>
     * This ensures that the `escapeCsv` method correctly handles quotes embedded within the input string.
     */
    @Test
    public void escapeCsvWithQuoteInMiddle() {
        CharSequence value = "some text\"and more text";
        CharSequence expected = "\"some text\"\"and more text\"";
        escapeCsv(value, expected);
    }

    /**
     * Tests the CSV escaping functionality where the input string contains quotes in the middle
     * and is already quoted. This ensures that the escaping function correctly handles such cases
     * by doubling the internal quotes while maintaining the surrounding quotes.
     * <p>
     * The source value: "some text"and more text"
     * Expected transformation: "some text""and more text"
     */
    @Test
    public void escapeCsvWithQuoteInMiddleAlreadyQuoted() {
        CharSequence value = "\"some text\"and more text\"";
        CharSequence expected = "\"some text\"\"and more text\"";
        escapeCsv(value, expected);
    }

    /**
     * Test method to verify the escaping of a CSV value that contains quoted words.
     * <p>
     * The method tests the functionality of the `escapeCsv` method from the StringUtil class
     * with a given input that includes quotes. The expected outcome is the CSV formatted string
     * where quotes inside the string are properly escaped.
     * <p>
     * The input is a CharSequence `value` which contains quoted content, and an
     * `expected` CharSequence which represents the correct escaped result.
     * <p>
     * The test ensures that the escaping function works correctly by asserting
     * the equality of the expected result and the actual result generated by
     * the `escapeCsv` method.
     */
    @Test
    public void escapeCsvWithQuotedWords() {
        CharSequence value = "\"foo\"\"goo\"";
        CharSequence expected = "\"foo\"\"goo\"";
        escapeCsv(value, expected);
    }

    /**
     * Tests the escaping of a CSV value that already contains an escaped quote.
     * <p>
     * This method verifies that when the input string contains a double quote
     * that is already escaped, the escaping function does not alter it further.
     * It uses the escapeCsv method to perform the escaping and checks the result
     * against an expected value to ensure correctness.
     */
    @Test
    public void escapeCsvWithAlreadyEscapedQuote() {
        CharSequence value = "foo\"\"goo";
        CharSequence expected = "foo\"\"goo";
        escapeCsv(value, expected);
    }

    /**
     * Tests the behavior of the `escapeCsv` method when the input ends with a quote character.
     * This method verifies that the `escapeCsv` method correctly escapes the input
     * by doubling the trailing quote and surrounding the entire input with quotes as expected.
     */
    @Test
    public void escapeCsvEndingWithQuote() {
        CharSequence value = "some\"";
        CharSequence expected = "\"some\"\"\"";
        escapeCsv(value, expected);
    }

    /**
     * Tests the method that escapes a given CSV field which contains double quotes.
     * Specifically, it handles the case where the CSV field starts or ends with a single double-quote character.
     * This test ensures the correct handling of escaping such characters by doubling them.
     */
    @Test
    public void escapeCsvWithSingleQuote() {
        CharSequence value = "\"";
        CharSequence expected = "\"\"\"\"";
        escapeCsv(value, expected);
    }

    /**
     * Tests the escaping of a CSV string that begins with a double quote
     * followed by a character using single quotes.
     * <p>
     * The method checks if the value "\"f" is correctly escaped to "\"\"\"f\""
     * using the escapeCsv method.
     */
    @Test
    public void escapeCsvWithSingleQuoteAndCharacter() {
        CharSequence value = "\"f";
        CharSequence expected = "\"\"\"f\"";
        escapeCsv(value, expected);
    }

    /**
     * Tests the behavior of the escapeCsv method when the input already contains an escaped quote.
     * In this test case, the input value has an already escaped quote, and we verify that
     * the escapeCsv method correctly escapes it again as needed.
     * <p>
     * Input:
     *   - value: A CharSequence containing the string with an already escaped quote.
     *   - expected: The expected result after processing by escapeCsv, which is
     *     the input string with the additional necessary escape.
     * <p>
     * The method asserts that the escapeCsv function produces the expected output.
     */
    @Test
    public void escapeCsvAlreadyEscapedQuote() {
        CharSequence value = "\"some\"\"";
        CharSequence expected = "\"some\"\"\"";
        escapeCsv(value, expected);
    }

    /**
     * Tests the `escapeCsv` method with a quoted CSV string.
     * <p>
     * The method verifies that the `escapeCsv` function can handle
     * a CSV value that is already enclosed in quotes correctly.
     * It passes the value "\"foo,goo\"" to the `escapeCsv` method
     * and asserts the result against the same expected value.
     */
    @Test
    public void escapeCsvQuoted() {
        CharSequence value = "\"foo,goo\"";
        escapeCsv(value, value);
    }

    /**
     * Tests the functionality of the escapeCsv method with a CharSequence containing
     * a line feed character. The method validates if escapeCsv correctly escapes the input
     * when the input contains a line feed, by comparing it with the expected escaped output.
     */
    @Test
    public void escapeCsvWithLineFeed() {
        CharSequence value = "some text\n more text";
        CharSequence expected = "\"some text\n more text\"";
        escapeCsv(value, expected);
    }

    /**
     * Tests the CSV escaping functionality for a single line feed character.
     * This method ensures that a line feed character is correctly escaped within a CSV context.
     * The expected behavior is that the single line feed character will be quoted in the output.
     */
    @Test
    public void escapeCsvWithSingleLineFeedCharacter() {
        CharSequence value = "\n";
        CharSequence expected = "\"\n\"";
        escapeCsv(value, expected);
    }

    /**
     * Test to verify the correct escaping of a CSV value that contains multiple line feed characters.
     * <p>
     * This test ensures that the escapeCsv method correctly handles and escapes a CharSequence that consists of multiple
     * line feed characters. The expected output should be properly quoted to maintain CSV format integrity.
     */
    @Test
    public void escapeCsvWithMultipleLineFeedCharacter() {
        CharSequence value = "\n\n";
        CharSequence expected = "\"\n\n\"";
        escapeCsv(value, expected);
    }

    /**
     * Tests the `escapeCsv` method with a given input that contains both quoted
     * characters and line feed characters. The method ensures that the CSV
     * escaping function works correctly when handling these characters.
     * <p>
     * The input value is a string containing a quote and a newline character.
     * The expected result is a properly escaped CSV string where the internal
     * quote is doubled, and the newline character is preserved.
     * <p>
     * The `escapeCsv` method is called with the input value and the expected
     * result, ensuring that the actual output matches the expected CSV format.
     */
    @Test
    public void escapeCsvWithQuotedAndLineFeedCharacter() {
        CharSequence value = " \" \n ";
        CharSequence expected = "\" \"\" \n \"";
        escapeCsv(value, expected);
    }

    /**
     * Tests the CSV escaping functionality with a string that ends with a line feed character.
     * Validates that the input string "testing\n" is correctly escaped to "\"testing\n\"".
     */
    @Test
    public void escapeCsvWithLineFeedAtEnd() {
        CharSequence value = "testing\n";
        CharSequence expected = "\"testing\n\"";
        escapeCsv(value, expected);
    }

    /**
     * Tests the escapeCsv method with a CSV string that contains a comma.
     * This test ensures that the escapeCsv method correctly wraps the input
     * string in quotes when a comma is present.
     * <p>
     * It invokes the escapeCsv method with the given CharSequence value
     * containing a comma and asserts that the returned value matches the expected output.
     */
    @Test
    public void escapeCsvWithComma() {
        CharSequence value = "test,ing";
        CharSequence expected = "\"test,ing\"";
        escapeCsv(value, expected);
    }

    /**
     * Tests the escaping of a single comma in CSV format.
     * The method assigns a comma to the value variable and the expected escaped representation "\",\""
     * to the expected variable. It then invokes the escapeCsv method to ensure the value is properly
     * escaped according to CSV format rules.
     */
    @Test
    public void escapeCsvWithSingleComma() {
        CharSequence value = ",";
        CharSequence expected = "\",\"";
        escapeCsv(value, expected);
    }

    /**
     * Tests the {@code escapeCsv} method when a single carriage return character is provided.
     * The expected outcome is that the carriage return character will be properly escaped
     * by enclosing it in double quotes.
     * <p>
     * This test verifies that the function correctly handles carriage return characters
     * by escaping them within CSV fields, ensuring the resultant string is as expected.
     */
    @Test
    public void escapeCsvWithSingleCarriageReturn() {
        CharSequence value = "\r";
        CharSequence expected = "\"\r\"";
        escapeCsv(value, expected);
    }

    /**
     * Tests the escapeCsv method when the input contains multiple carriage return characters.
     * Ensures that the multiple carriage return characters are properly escaped according to CSV rules.
     */
    @Test
    public void escapeCsvWithMultipleCarriageReturn() {
        CharSequence value = "\r\r";
        CharSequence expected = "\"\r\r\"";
        escapeCsv(value, expected);
    }

    /**
     *
     */
    @Test
    public void escapeCsvWithCarriageReturn() {
        CharSequence value = "some text\r more text";
        CharSequence expected = "\"some text\r more text\"";
        escapeCsv(value, expected);
    }

    /**
     * Tests the CSV escaping functionality specifically for input containing a quoted and carriage return character.
     * <p>
     * This method creates a CharSequence with a quoted and carriage return character, and another CharSequence
     * that represents the expected output after escaping. It then calls the escapeCsv method to verify that the
     * escaping is performed correctly.
     */
    @Test
    public void escapeCsvWithQuotedAndCarriageReturnCharacter() {
        CharSequence value = "\"\r";
        CharSequence expected = "\"\"\"\r\"";
        escapeCsv(value, expected);
    }

    /**
     * Tests the escapeCsv method for a string that ends with a carriage return character.
     * This test ensures that the carriage return at the end of the string is correctly escaped within quotes.
     * <p>
     * The test sets the input value to a CharSequence containing "testing\r" and the expected output to
     * a CharSequence containing "\"testing\r\"". It then calls the escapeCsv method with these parameters.
     * Asserts that the actual escaped value matches the expected escaped value.
     */
    @Test
    public void escapeCsvWithCarriageReturnAtEnd() {
        CharSequence value = "testing\r";
        CharSequence expected = "\"testing\r\"";
        escapeCsv(value, expected);
    }

    /**
     * Tests the escaping of a CSV value that contains a Carriage Return and Line Feed (CRLF) character.
     * This method ensures that a given CharSequence value with CRLF characters is properly escaped
     * to meet CSV format standards.
     * <p>
     * The test verifies that the escapeCsv method correctly escapes the CRLF characters by placing
     * them within quotes.
     * <p>
     * The expected output for the given input value containing CRLF is: "\"\r\n\"".
     */
    @Test
    public void escapeCsvWithCRLFCharacter() {
        CharSequence value = "\r\n";
        CharSequence expected = "\"\r\n\"";
        escapeCsv(value, expected);
    }

    /**
     * Test method for verifying the behavior of the `escapeCsv` method with the trimming option enabled.
     * This method checks various scenarios to ensure that the CSV escaping is performed correctly
     * while optional white space characters are trimmed when specified.
     * <p>
     * The test cases include:
     * - Empty strings
     * - Strings with leading and trailing whitespaces
     * - Strings containing tab characters
     * - Strings containing commas
     * - Strings with embedded quotes
     * - Strings with newline characters
     * <p>
     * The assertions verify that the `escapeCsv` method returns the expected results for each case.
     */
    @Test
    public void escapeCsvWithTrimming() {
        assertSame("", StringUtil.escapeCsv("", true));
        assertSame("ab", StringUtil.escapeCsv("ab", true));

        escapeCsvWithTrimming("", "");
        escapeCsvWithTrimming(" \t ", "");
        escapeCsvWithTrimming("ab", "ab");
        escapeCsvWithTrimming("a b", "a b");
        escapeCsvWithTrimming(" \ta \tb", "a \tb");
        escapeCsvWithTrimming("a \tb \t", "a \tb");
        escapeCsvWithTrimming("\t a \tb \t", "a \tb");
        escapeCsvWithTrimming("\"\t a b \"", "\"\t a b \"");
        escapeCsvWithTrimming(" \"\t a b \"\t", "\"\t a b \"");
        escapeCsvWithTrimming(" testing\t\n ", "\"testing\t\n\"");
        escapeCsvWithTrimming("\ttest,ing ", "\"test,ing\"");
    }

    /**
     * Tests the `StringUtil.escapeCsv` method to ensure it returns the same string object
     * when no changes are made to the input string. This method checks various cases
     * including strings with and without quotes, as well as strings with special characters.
     * <p>
     * The method verifies the garbage-free property of the `escapeCsv` method by asserting
     * that the returned object is the same as the input object when expected.
     */
    @Test
    public void escapeCsvGarbageFree() {
        // 'StringUtil#escapeCsv()' should return same string object if string didn't changing.
        assertSame("1", StringUtil.escapeCsv("1", true));
        assertSame(" 123 ", StringUtil.escapeCsv(" 123 ", false));
        assertSame("\" 123 \"", StringUtil.escapeCsv("\" 123 \"", true));
        assertSame("\"\"", StringUtil.escapeCsv("\"\"", true));
        assertSame("123 \"\"", StringUtil.escapeCsv("123 \"\"", true));
        assertSame("123\"\"321", StringUtil.escapeCsv("123\"\"321", true));
        assertSame("\"123\"\"321\"", StringUtil.escapeCsv("\"123\"\"321\"", true));
    }

    /**
     * Tests the unescapeCsv method for various input strings.
     * <p>
     * The method verifies the correct unescaping of CSV-escaped strings,
     * checking different scenarios, such as empty strings, quoted strings,
     * and special characters like newlines and carriage returns.
     */
    @Test
    public void testUnescapeCsv() {
        assertEquals("", unescapeCsv(""));
        assertEquals("\"", unescapeCsv("\"\"\"\""));
        assertEquals("\"\"", unescapeCsv("\"\"\"\"\"\""));
        assertEquals("\"\"\"", unescapeCsv("\"\"\"\"\"\"\"\""));
        assertEquals("\"netty\"", unescapeCsv("\"\"\"netty\"\"\""));
        assertEquals("netty", unescapeCsv("netty"));
        assertEquals("netty", unescapeCsv("\"netty\""));
        assertEquals("\r", unescapeCsv("\"\r\""));
        assertEquals("\n", unescapeCsv("\"\n\""));
        assertEquals("hello,netty", unescapeCsv("\"hello,netty\""));
    }

    /**
     * Tests the unescapeCsv method to ensure it throws an IllegalArgumentException
     * when given a single double quote character as input.
     * <p>
     * The test is implemented using the assertThrows method to verify
     * that the appropriate exception is thrown during the execution
     * of the unescapeCsv method.
     * <p>
     * This test is specifically designed to handle the edge case where
     * the input consists of an incomplete quoted string.
     */
    @Test
    public void unescapeCsvWithSingleQuote() {
        assertThrows(IllegalArgumentException.class, new Executable() {
            @Override
            public void execute() {
                unescapeCsv("\"");
            }
        });
    }

    /**
     * Test case for unescapeCsv method to verify behavior when the input contains an odd number of quotes.
     * <p>
     * This test expects an IllegalArgumentException to be thrown when unescapeCsv is called with the input "\"\"\"".
     * It uses JUnit's assertThrows to check if the exception is properly thrown.
     */
    @Test
    public void unescapeCsvWithOddQuote() {
        assertThrows(IllegalArgumentException.class, new Executable() {
            @Override
            public void execute() {
                unescapeCsv("\"\"\"");
            }
        });
    }

    /**
     * Tests the behavior of the unescapeCsv method when the input string contains
     * a carriage return (CR) character and is not quoted.
     * The expected outcome is that the method throws an IllegalArgumentException.
     * <p>
     * This test is needed because a CR character in unquoted CSV data could indicate
     * an improperly formatted CSV string. The method being tested should validate the
     * input and raise an appropriate exception for invalid cases.
     * <p>
     * The method uses JUnit's assertThrows to verify that an IllegalArgumentException
     * is indeed thrown when unescapeCsv is called with the input '\r'.
     */
    @Test
    public void unescapeCsvWithCRAndWithoutQuote() {
        assertThrows(IllegalArgumentException.class, new Executable() {
            @Override
            public void execute() {
                unescapeCsv("\r");
            }
        });
    }

    /**
     * Tests the {@code unescapeCsv} method when a line feed character ("\n") is provided without quotes.
     * This method ensures that an IllegalArgumentException is thrown in this scenario.
     * <p>
     * The test uses the method which takes {@link IllegalArgumentException}
     * as an argument to assert that this exception is thrown during the execution of the {@code unescapeCsv} method.
     *
     * @throws IllegalArgumentException if the input contains a line feed character without quotes
     */
    @Test
    public void unescapeCsvWithLFAndWithoutQuote() {
        assertThrows(IllegalArgumentException.class, new Executable() {
            @Override
            public void execute() {
                unescapeCsv("\n");
            }
        });
    }

    /**
     * Unit test method which verifies that unescaping a CSV string consisting
     * only of a comma without enclosing quotes results in an
     * IllegalArgumentException being thrown.
     * <p>
     * The test uses JUnit's assertThrows to expect the IllegalArgumentException
     * when the method unescapeCsv is called with a single comma.
     */
    @Test
    public void unescapeCsvWithCommaAndWithoutQuote() {
        assertThrows(IllegalArgumentException.class, new Executable() {
            @Override
            public void execute() {
                unescapeCsv(",");
            }
        });
    }

    /**
     * Test method validating the escaping and unescaping of CSV strings.
     * This method performs various assertions to ensure that the escape and unescape
     * functionalities are working correctly.
     */
    @Test
    public void escapeCsvAndUnEscapeCsv() {
        assertEscapeCsvAndUnEscapeCsv("");
        assertEscapeCsvAndUnEscapeCsv("netty");
        assertEscapeCsvAndUnEscapeCsv("hello,netty");
        assertEscapeCsvAndUnEscapeCsv("hello,\"netty\"");
        assertEscapeCsvAndUnEscapeCsv("\"");
        assertEscapeCsvAndUnEscapeCsv(",");
        assertEscapeCsvAndUnEscapeCsv("\r");
        assertEscapeCsvAndUnEscapeCsv("\n");
    }

    /**
     * Tests various scenarios for unescaping CSV fields.
     * This method verifies that CSV fields are correctly unescaped, turning
     * the CSV formatted strings into their original list of fields.
     * <p>
     * Asserts that:
     * - An empty string is unescaped to a list containing a single empty string.
     * - A comma results in a list with two empty strings.
     * - A string with one value followed by a comma results in a list with the value and an empty string.
     * - A comma followed by one value results in a list with an empty string and the value.
     * - Double quotes enclosed in double quotes result in a single quote character in the list.
     * - Multiple double quotes sequences are properly unescaped into respective quote characters.
     * - Single fields without commas or quotes remain unchanged.
     * - Fields separated by a comma result in respective values in the list.
     * - Quoted fields with comma inside are correctly interpreted as a single field with a comma.
     * - Multiple quoted fields with various characters are properly unescaped and parsed into original contents.
     * - Special characters like carriage return and newline within quoted fields are preserved in the list.
     */
    @Test
    public void testUnescapeCsvFields() {
        assertEquals(Collections.singletonList(""), unescapeCsvFields(""));
        assertEquals(Arrays.asList("", ""), unescapeCsvFields(","));
        assertEquals(Arrays.asList("a", ""), unescapeCsvFields("a,"));
        assertEquals(Arrays.asList("", "a"), unescapeCsvFields(",a"));
        assertEquals(Collections.singletonList("\""), unescapeCsvFields("\"\"\"\""));
        assertEquals(Arrays.asList("\"", "\""), unescapeCsvFields("\"\"\"\",\"\"\"\""));
        assertEquals(Collections.singletonList("netty"), unescapeCsvFields("netty"));
        assertEquals(Arrays.asList("hello", "netty"), unescapeCsvFields("hello,netty"));
        assertEquals(Collections.singletonList("hello,netty"), unescapeCsvFields("\"hello,netty\""));
        assertEquals(Arrays.asList("hello", "netty"), unescapeCsvFields("\"hello\",\"netty\""));
        assertEquals(Arrays.asList("a\"b", "c\"d"), unescapeCsvFields("\"a\"\"b\",\"c\"\"d\""));
        assertEquals(Arrays.asList("a\rb", "c\nd"), unescapeCsvFields("\"a\rb\",\"c\nd\""));
    }

    /**
     * Tests the behavior of the `unescapeCsvFields` method when encountering a comma
     * followed by a carriage return without enclosing quotes.
     * This method validates that attempting to unescape such a string
     * throws an `IllegalArgumentException`.
     */
    @Test
    public void unescapeCsvFieldsWithCRWithoutQuote() {
        assertThrows(IllegalArgumentException.class, new Executable() {
            @Override
            public void execute() {
                unescapeCsvFields("a,\r");
            }
        });
    }

    /**
     * Unit test for uncapping CSV fields containing line feed characters without enclosing quotes.
     * The test ensures that an IllegalArgumentException is thrown when the input string contains a line feed character
     * without quotes.
     * <p>
     * The method under test is unescapeCsvFields, which presumably handles unescaping CSV data. This specific test case
     * validates the behavior of the method when encountering a line feed character that is not properly quoted.
     * <p>
     * The method asserts that calling unescapeCsvFields with an input of "a,\r" throws an IllegalArgumentException.
     */
    @Test
    public void unescapeCsvFieldsWithLFWithoutQuote() {
        assertThrows(IllegalArgumentException.class, new Executable() {
            @Override
            public void execute() {
                unescapeCsvFields("a,\r");
            }
        });
    }

    /**
     * Tests the behavior of the `unescapeCsvFields` method when a CSV field ends with an unescaped quote.
     * This method expects an `IllegalArgumentException` to be thrown due to the invalid input.
     */
    @Test
    public void unescapeCsvFieldsWithQuote() {
        assertThrows(IllegalArgumentException.class, new Executable() {
            @Override
            public void execute() {
                unescapeCsvFields("a,\"");
            }
        });
    }

    /**
     * Tests the `unescapeCsvFields` method with an input string starting with a quote,
     * followed by a comma and another character. The expected behavior is to throw an
     * `IllegalArgumentException`.
     */
    @Test
    public void unescapeCsvFieldsWithQuote2() {
        assertThrows(IllegalArgumentException.class, new Executable() {
            @Override
            public void execute() {
                unescapeCsvFields("\",a");
            }
        });
    }

    /**
     * Tests the behavior of the unescapeCsvFields method when encountering invalid CSV fields
     * containing quotes that are not properly escaped.
     * <p>
     * This test asserts that an {@link IllegalArgumentException} is thrown when the
     * input CSV field contains a mismatched quote.
     * <p>
     * The specific input that is tested is "a"b,a" which contains an unescaped quote
     * in the middle of the field, violating CSV format rules and triggering an exception.
     * <p>
     * The method uses the method to verify that
     * {@link IllegalArgumentException} is thrown as expected.
     */
    @Test
    public void unescapeCsvFieldsWithQuote3() {
        assertThrows(IllegalArgumentException.class, new Executable() {
            @Override
            public void execute() {
                unescapeCsvFields("a\"b,a");
            }
        });
    }

    /**
     * Tests the simple class name extraction functionality
     * by calling the testSimpleClassName method with String.class
     * and validating the extracted name.
     *
     * @throws Exception If the class name extraction fails.
     */
    @Test
    public void testSimpleClassName() throws Exception {
        testSimpleClassName(String.class);
    }

    /**
     * Tests the simple inner class name resolution.
     *
     * @throws Exception if an error occurs during the test
     */
    @Test
    public void testSimpleInnerClassName() throws Exception {
        testSimpleClassName(TestClass.class);
    }

    /**
     * Tests the StringUtil.endsWith method.
     * This test verifies the behavior of the endsWith method with different input strings.
     * The method checks whether a string ends with a specified character.
     * <p>
     * It includes the following cases:
     * - An empty string with a non-matching character.
     * - A single character string that matches the character.
     * - A string ending with the specified character.
     * - A string that does not end with the specified character.
     * - A string where the specified character is at the beginning or in the middle.
     */
    @Test
    public void testEndsWith() {
        assertFalse(StringUtil.endsWith("", 'u'));
        assertTrue(StringUtil.endsWith("u", 'u'));
        assertTrue(StringUtil.endsWith("-u", 'u'));
        assertFalse(StringUtil.endsWith("-", 'u'));
        assertFalse(StringUtil.endsWith("u-", 'u'));
    }

    /**
     * This method ensures that the StringUtil.trimOws function correctly trims
     * optional whitespace (OWS) from inputs while preserving other characters.
     * Various input cases are tested, including empty strings, strings with
     * leading and trailing whitespace, and strings with embedded tabs.
     */
    @Test
    public void trimOws() {
        assertSame("", StringUtil.trimOws(""));
        assertEquals("", StringUtil.trimOws(" \t "));
        assertSame("a", StringUtil.trimOws("a"));
        assertEquals("a", StringUtil.trimOws(" a"));
        assertEquals("a", StringUtil.trimOws("a "));
        assertEquals("a", StringUtil.trimOws(" a "));
        assertSame("abc", StringUtil.trimOws("abc"));
        assertEquals("abc", StringUtil.trimOws("\tabc"));
        assertEquals("abc", StringUtil.trimOws("abc\t"));
        assertEquals("abc", StringUtil.trimOws("\tabc\t"));
        assertSame("a\t b", StringUtil.trimOws("a\t b"));
        assertEquals("", StringUtil.trimOws("\t ").toString());
        assertEquals("a b", StringUtil.trimOws("\ta b \t").toString());
    }

    /**
     * Tests the {@code StringUtil.join} method by asserting the returned string
     * against expected outcomes for various input collections.
     * <p>
     * This method includes tests for:
     * <ul>
     *     <li>Empty collection input</li>
     *     <li>Single-element collection input</li>
     *     <li>Multi-element collection input</li>
     *     <li>Collection input with {@code null} values</li>
     * </ul>
     */
    @Test
    public void testJoin() {
        assertEquals("",
                StringUtil.join(",", Collections.emptyList()).toString());
        assertEquals("a",
                StringUtil.join(",", Collections.singletonList("a")).toString());
        assertEquals("a,b",
                StringUtil.join(",", Arrays.asList("a", "b")).toString());
        assertEquals("a,b,c",
                StringUtil.join(",", Arrays.asList("a", "b", "c")).toString());
        assertEquals("a,b,c,null,d",
                StringUtil.join(",", Arrays.asList("a", "b", "c", null, "d")).toString());
    }

    /**
     * Tests the isNullOrEmpty method to ensure it correctly identifies null or empty strings.
     * <p>
     * The method validates several string conditions:
     * - Null string
     * - Empty string
     * - String constant representing an empty string
     * - Strings composed of various whitespace characters (space, tab, newline)
     * - Non-empty strings containing characters
     * <p>
     * The assertions are expected to behave as follows:
     * - True for null, empty, and an empty string constant.
     * - False for strings with whitespace or other characters, including newline.
     * <p>
     * Method is annotated with @Test indicating it's a test case in a unit testing framework.
     */
    @Test
    public void testIsNullOrEmpty() {
        assertTrue(isNullOrEmpty(null));
        assertTrue(isNullOrEmpty(""));
        assertTrue(isNullOrEmpty(StringUtil.EMPTY_STRING));
        assertFalse(isNullOrEmpty(" "));
        assertFalse(isNullOrEmpty("\t"));
        assertFalse(isNullOrEmpty("\n"));
        assertFalse(isNullOrEmpty("foo"));
        assertFalse(isNullOrEmpty(NEWLINE));
    }

    /**
     * Tests the `indexOfWhiteSpace` method with various input strings and starting indices.
     * <p>
     * This test case validates several scenarios to ensure the method is correctly identifying the
     * index of whitespace characters within the string based on the starting index provided.
     * <p>
     * It covers:
     * - Empty strings
     * - Single whitespace characters like space, newline, and tab
     * - Mixed strings with different whitespace characters and regular text
     * - Edge cases with start indices out of bounds or maximum integer values
     */
    @Test
    public void testIndexOfWhiteSpace() {
        assertEquals(-1, indexOfWhiteSpace("", 0));
        assertEquals(0, indexOfWhiteSpace(" ", 0));
        assertEquals(-1, indexOfWhiteSpace(" ", 1));
        assertEquals(0, indexOfWhiteSpace("\n", 0));
        assertEquals(-1, indexOfWhiteSpace("\n", 1));
        assertEquals(0, indexOfWhiteSpace("\t", 0));
        assertEquals(-1, indexOfWhiteSpace("\t", 1));
        assertEquals(3, indexOfWhiteSpace("foo\r\nbar", 1));
        assertEquals(-1, indexOfWhiteSpace("foo\r\nbar", 10));
        assertEquals(7, indexOfWhiteSpace("foo\tbar\r\n", 6));
        assertEquals(-1, indexOfWhiteSpace("foo\tbar\r\n", Integer.MAX_VALUE));
    }

    /**
     * Tests the method indexOfNonWhiteSpace which identifies the index of the first non-whitespace character in a string starting from a given offset.
     * <p>
     * This test includes various cases:
     * - Empty string input.
     * - Strings with only whitespace characters.
     * - Strings with whitespace followed by non-whitespace characters.
     * - Strings with non-whitespace characters followed by whitespace.
     * - Out of bounds start index.
     */
    @Test
    public void testIndexOfNonWhiteSpace() {
        assertEquals(-1, indexOfNonWhiteSpace("", 0));
        assertEquals(-1, indexOfNonWhiteSpace(" ", 0));
        assertEquals(-1, indexOfNonWhiteSpace(" \t", 0));
        assertEquals(-1, indexOfNonWhiteSpace(" \t\r\n", 0));
        assertEquals(2, indexOfNonWhiteSpace(" \tfoo\r\n", 0));
        assertEquals(2, indexOfNonWhiteSpace(" \tfoo\r\n", 1));
        assertEquals(4, indexOfNonWhiteSpace(" \tfoo\r\n", 4));
        assertEquals(-1, indexOfNonWhiteSpace(" \tfoo\r\n", 10));
        assertEquals(-1, indexOfNonWhiteSpace(" \tfoo\r\n", Integer.MAX_VALUE));
    }

    private static final class TestClass {
    }
}
