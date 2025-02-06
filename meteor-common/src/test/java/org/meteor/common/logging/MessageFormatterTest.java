package org.meteor.common.logging;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

public class MessageFormatterTest {

    /**
     * Tests the MessageFormatter.format method with a null message pattern.
     * Verifies that the method returns null when passed a null pattern and a non-null argument.
     */
    @Test
    public void testNull() {
        String result = MessageFormatter.format(null, 1).getMessage();
        assertNull(result);
    }

    /**
     * Ensures that null parameters are handled properly by the MessageFormatter and do not cause errors.
     * The method verifies various scenarios where null values are passed to the formatter, both as single
     * values and within arrays, and asserts that the resulting messages are correctly formed.
     */
    @Test
    public void nullParametersShouldBeHandledWithoutBarfing() {
        String result = MessageFormatter.format("Value is {}.", null).getMessage();
        assertEquals("Value is null.", result);

        result = MessageFormatter.format("Val1 is {}, val2 is {}.", null, null).getMessage();
        assertEquals("Val1 is null, val2 is null.", result);

        result = MessageFormatter.format("Val1 is {}, val2 is {}.", 1, null).getMessage();
        assertEquals("Val1 is 1, val2 is null.", result);

        result = MessageFormatter.format("Val1 is {}, val2 is {}.", null, 2).getMessage();
        assertEquals("Val1 is null, val2 is 2.", result);

        result = MessageFormatter.arrayFormat(
                "Val1 is {}, val2 is {}, val3 is {}", new Integer[]{null, null, null}).getMessage();
        assertEquals("Val1 is null, val2 is null, val3 is null", result);

        result = MessageFormatter.arrayFormat(
                "Val1 is {}, val2 is {}, val3 is {}", new Integer[]{null, 2, 3}).getMessage();
        assertEquals("Val1 is null, val2 is 2, val3 is 3", result);

        result = MessageFormatter.arrayFormat(
                "Val1 is {}, val2 is {}, val3 is {}", new Integer[]{null, null, 3}).getMessage();
        assertEquals("Val1 is null, val2 is null, val3 is 3", result);
    }

    /**
     * This method tests the behavior of the MessageFormatter by verifying that a single parameter
     * is correctly handled within various message format templates. It checks the formatted output
     * by asserting the expected results for different input scenarios, including:
     * <p>
     * - Basic substitution: replacing a placeholder with a given parameter.
     * - Ignoring unmatched placeholders.
     * - Handling placeholders at the beginning or end of the template string.
     * - Ignoring templates without placeholders.
     * - Handling mismatched or incorrect placeholder formats.
     * - Escaping placeholders so they are not replaced.
     * - Combining escaped and non-escaped placeholders.
     * - Escaping the escape character itself.
     * <p>
     * It performs assertions to validate that the MessageFormatter formats the messages correctly
     * according to the defined rules.
     */
    @Test
    public void verifyOneParameterIsHandledCorrectly() {
        String result = MessageFormatter.format("Value is {}.", 3).getMessage();
        assertEquals("Value is 3.", result);

        result = MessageFormatter.format("Value is {", 3).getMessage();
        assertEquals("Value is {", result);

        result = MessageFormatter.format("{} is larger than 2.", 3).getMessage();
        assertEquals("3 is larger than 2.", result);

        result = MessageFormatter.format("No subst", 3).getMessage();
        assertEquals("No subst", result);

        result = MessageFormatter.format("Incorrect {subst", 3).getMessage();
        assertEquals("Incorrect {subst", result);

        result = MessageFormatter.format("Value is {bla} {}", 3).getMessage();
        assertEquals("Value is {bla} 3", result);

        result = MessageFormatter.format("Escaped \\{} subst", 3).getMessage();
        assertEquals("Escaped {} subst", result);

        result = MessageFormatter.format("{Escaped", 3).getMessage();
        assertEquals("{Escaped", result);

        result = MessageFormatter.format("\\{}Escaped", 3).getMessage();
        assertEquals("{}Escaped", result);

        result = MessageFormatter.format("File name is {{}}.", "App folder.zip").getMessage();
        assertEquals("File name is {App folder.zip}.", result);

        // escaping the escape character
        result = MessageFormatter.format("File name is C:\\\\{}.", "App folder.zip").getMessage();
        assertEquals("File name is C:\\App folder.zip.", result);
    }

    /**
     * Tests the formatting functionalities of the MessageFormatter with two placeholders.
     * This method verifies that the formatted messages correctly replace placeholders with provided parameters.
     * The assertions check various formats and escaping scenarios.
     */
    @Test
    public void testTwoParameters() {
        String result = MessageFormatter.format("Value {} is smaller than {}.", 1, 2).getMessage();
        assertEquals("Value 1 is smaller than 2.", result);

        result = MessageFormatter.format("Value {} is smaller than {}", 1, 2).getMessage();
        assertEquals("Value 1 is smaller than 2", result);

        result = MessageFormatter.format("{}{}", 1, 2).getMessage();
        assertEquals("12", result);

        result = MessageFormatter.format("Val1={}, Val2={", 1, 2).getMessage();
        assertEquals("Val1=1, Val2={", result);

        result = MessageFormatter.format("Value {} is smaller than \\{}", 1, 2).getMessage();
        assertEquals("Value 1 is smaller than {}", result);

        result = MessageFormatter.format("Value {} is smaller than \\{} tail", 1, 2).getMessage();
        assertEquals("Value 1 is smaller than {} tail", result);

        result = MessageFormatter.format("Value {} is smaller than \\{", 1, 2).getMessage();
        assertEquals("Value 1 is smaller than \\{", result);

        result = MessageFormatter.format("Value {} is smaller than {tail", 1, 2).getMessage();
        assertEquals("Value 1 is smaller than {tail", result);

        result = MessageFormatter.format("Value \\{} is smaller than {}", 1, 2).getMessage();
        assertEquals("Value {} is smaller than 1", result);
    }

    /**
     * Tests the behavior of the MessageFormatter when an exception is thrown
     * by the toString() method of an object.
     * <p>
     * This test creates an anonymous object whose toString() method always throws
     * an IllegalStateException. The method then formats a string using this object
     * and verifies that the resulting message handles the exception properly by
     * substituting the placeholder with "[FAILED toString()]".
     */
    @Test
    public void testExceptionIn_toString() {
        Object o = new Object() {
            @Override
            public String toString() {
                throw new IllegalStateException("a");
            }
        };
        String result = MessageFormatter.format("Troublesome object {}", o).getMessage();
        assertEquals("Troublesome object [FAILED toString()]", result);
    }

    /**
     * Tests the behavior of the MessageFormatter.arrayFormat method when passed a null array of arguments.
     * <p>
     * The test method verifies that when the argument array is null, the format string remains unchanged.
     * It asserts that the returned message is the same as the original format strings provided, which
     * include different numbers of placeholders.
     * <p>
     * Asserts:
     * - The result matches the original format string when the argument array is null.
     */
    @Test
    public void testNullArray() {
        String msg0 = "msg0";
        String msg1 = "msg1 {}";
        String msg2 = "msg2 {} {}";
        String msg3 = "msg3 {} {} {}";

        Object[] args = null;

        String result = MessageFormatter.arrayFormat(msg0, args).getMessage();
        assertEquals(msg0, result);

        result = MessageFormatter.arrayFormat(msg1, args).getMessage();
        assertEquals(msg1, result);

        result = MessageFormatter.arrayFormat(msg2, args).getMessage();
        assertEquals(msg2, result);

        result = MessageFormatter.arrayFormat(msg3, args).getMessage();
        assertEquals(msg3, result);
    }

    /**
     * Tests the case when the parameters are supplied in a single array to the MessageFormatter.
     * The method covers various scenarios such as:
     * <p>
     * 1. Basic formatting with multiple placeholders.
     * 2. Consecutive placeholders without delimiters.
     * 3. Extra array elements ignored in formatting.
     * 4. Format strings with unbalanced brackets.
     * <p>
     * This ensures that MessageFormatter correctly handles array inputs for formatted message strings.
     */
    // tests the case when the parameters are supplied in a single array
    @Test
    public void testArrayFormat() {
        Integer[] ia0 = {1, 2, 3};

        String result = MessageFormatter.arrayFormat("Value {} is smaller than {} and {}.", ia0).getMessage();
        assertEquals("Value 1 is smaller than 2 and 3.", result);

        result = MessageFormatter.arrayFormat("{}{}{}", ia0).getMessage();
        assertEquals("123", result);

        result = MessageFormatter.arrayFormat("Value {} is smaller than {}.", ia0).getMessage();
        assertEquals("Value 1 is smaller than 2.", result);

        result = MessageFormatter.arrayFormat("Value {} is smaller than {}", ia0).getMessage();
        assertEquals("Value 1 is smaller than 2", result);

        result = MessageFormatter.arrayFormat("Val={}, {, Val={}", ia0).getMessage();
        assertEquals("Val=1, {, Val=2", result);

        result = MessageFormatter.arrayFormat("Val={}, {, Val={}", ia0).getMessage();
        assertEquals("Val=1, {, Val=2", result);

        result = MessageFormatter.arrayFormat("Val1={}, Val2={", ia0).getMessage();
        assertEquals("Val1=1, Val2={", result);
    }

    /**
     * Test the formatting of various array types using the MessageFormatter.
     * <p>
     * This method will assert that the MessageFormatter correctly formats messages when provided
     * with different array types such as Integer, byte, int, float, and double.
     * <p>
     * The method accomplishes this by:
     * 1. Creating specific arrays and formatting them with the MessageFormatter.
     * 2. Asserting that the formatted output matches the expected string representations.
     */
    @Test
    public void testArrayValues() {
        Integer[] p1 = {2, 3};

        String result = MessageFormatter.format("{}{}", 1, p1).getMessage();
        assertEquals("1[2, 3]", result);

        // Integer[]
        result = MessageFormatter.arrayFormat("{}{}", new Object[]{"a", p1}).getMessage();
        assertEquals("a[2, 3]", result);

        // byte[]
        result = MessageFormatter.arrayFormat("{}{}", new Object[]{"a", new byte[]{1, 2}}).getMessage();
        assertEquals("a[1, 2]", result);

        // int[]
        result = MessageFormatter.arrayFormat("{}{}", new Object[]{"a", new int[]{1, 2}}).getMessage();
        assertEquals("a[1, 2]", result);

        // float[]
        result = MessageFormatter.arrayFormat("{}{}", new Object[]{"a", new float[]{1, 2}}).getMessage();
        assertEquals("a[1.0, 2.0]", result);

        // double[]
        result = MessageFormatter.arrayFormat("{}{}", new Object[]{"a", new double[]{1, 2}}).getMessage();
        assertEquals("a[1.0, 2.0]", result);
    }

    /**
     * Test method for verifying the correct handling and formatting of multi-dimensional array values
     * in the MessageFormatter class's arrayFormat method.
     * <p>
     * This test checks the formatting of arrays with different data types and dimensions, including:
     * - 1-dimensional and 2-dimensional Integer arrays
     * - 2-dimensional int arrays
     * - 2-dimensional float arrays
     * - Multi-dimensional Object arrays
     * - Nested 3-dimensional Object arrays
     * - Combined Byte and Short arrays within an Object array
     * <p>
     * The assertions validate that the formatted string output matches the expected string representation
     * of the input multidimensional arrays.
     */
    @Test
    public void testMultiDimensionalArrayValues() {
        Integer[] ia0 = {1, 2, 3};
        Integer[] ia1 = {10, 20, 30};

        Integer[][] multiIntegerA = {ia0, ia1};
        String result = MessageFormatter.arrayFormat("{}{}", new Object[]{"a", multiIntegerA}).getMessage();
        assertEquals("a[[1, 2, 3], [10, 20, 30]]", result);

        int[][] multiIntA = {{1, 2}, {10, 20}};
        result = MessageFormatter.arrayFormat("{}{}", new Object[]{"a", multiIntA}).getMessage();
        assertEquals("a[[1, 2], [10, 20]]", result);

        float[][] multiFloatA = {{1, 2}, {10, 20}};
        result = MessageFormatter.arrayFormat("{}{}", new Object[]{"a", multiFloatA}).getMessage();
        assertEquals("a[[1.0, 2.0], [10.0, 20.0]]", result);

        Object[][] multiOA = {ia0, ia1};
        result = MessageFormatter.arrayFormat("{}{}", new Object[]{"a", multiOA}).getMessage();
        assertEquals("a[[1, 2, 3], [10, 20, 30]]", result);

        Object[][][] _3DOA = {multiOA, multiOA};
        result = MessageFormatter.arrayFormat("{}{}", new Object[]{"a", _3DOA}).getMessage();
        assertEquals("a[[[1, 2, 3], [10, 20, 30]], [[1, 2, 3], [10, 20, 30]]]", result);

        Byte[] ba0 = {0, Byte.MAX_VALUE, Byte.MIN_VALUE};
        Short[] sa0 = {0, Short.MIN_VALUE, Short.MAX_VALUE};
        result = MessageFormatter.arrayFormat("{}\\{}{}", new Object[]{new Object[]{ba0, sa0}, ia1}).getMessage();
        assertEquals("[[0, 127, -128], [0, -32768, 32767]]{}[10, 20, 30]", result);
    }

    /**
     * Tests the handling of cyclic arrays by the MessageFormatter.
     * <p>
     * Ensures that cyclic references within arrays are properly detected
     * and represented in the output message string without causing infinite loops.
     * The expected output for a cyclic array is a representation with "[[...]",
     * indicating the cyclic nature of the array.
     */
    @Test
    public void testCyclicArrays() {
        Object[] cyclicA = new Object[1];
        cyclicA[0] = cyclicA;
        assertEquals("[[...]]", MessageFormatter.arrayFormat("{}", cyclicA).getMessage());

        Object[] a = new Object[2];
        a[0] = 1;
        Object[] c = {3, a};
        Object[] b = {2, c};
        a[1] = b;
        assertEquals("1[2, [3, [1, [...]]]]",
                MessageFormatter.arrayFormat("{}{}", a).getMessage());
    }

    /**
     * Test method to validate the behavior of the MessageFormatter when formatting messages with arrays that include a Throwable instance.
     * <p>
     * This method tests various scenarios where the message format and the length of the provided array differ. It ensures that the formatted message
     * and the throwable, if present, are correctly extracted and compared against expected values.
     * <p>
     * The method performs the following assertions:
     * 1. Checks the formatted message and throwable extraction when the message template includes placeholders.
     * 2. Verifies the behavior when message template placeholders match the length of the array exactly.
     * 3. Assesses how the formatter deals with mismatched array length and placeholders in the template.
     * 4. Tests the escape sequences in the message template.
     * 5. Confirms that no throwable is associated if the Throwable appears as a formatted value.
     */
    @Test
    public void testArrayThrowable() {
        FormattingTuple ft;
        Throwable t = new Throwable();
        Object[] ia = {1, 2, 3, t};

        ft = MessageFormatter.arrayFormat("Value {} is smaller than {} and {}.", ia);
        assertEquals("Value 1 is smaller than 2 and 3.", ft.getMessage());
        assertEquals(t, ft.getThrowable());

        ft = MessageFormatter.arrayFormat("{}{}{}", ia);
        assertEquals("123", ft.getMessage());
        assertEquals(t, ft.getThrowable());

        ft = MessageFormatter.arrayFormat("Value {} is smaller than {}.", ia);
        assertEquals("Value 1 is smaller than 2.", ft.getMessage());
        assertEquals(t, ft.getThrowable());

        ft = MessageFormatter.arrayFormat("Value {} is smaller than {}", ia);
        assertEquals("Value 1 is smaller than 2", ft.getMessage());
        assertEquals(t, ft.getThrowable());

        ft = MessageFormatter.arrayFormat("Val={}, {, Val={}", ia);
        assertEquals("Val=1, {, Val=2", ft.getMessage());
        assertEquals(t, ft.getThrowable());

        ft = MessageFormatter.arrayFormat("Val={}, \\{, Val={}", ia);
        assertEquals("Val=1, \\{, Val=2", ft.getMessage());
        assertEquals(t, ft.getThrowable());

        ft = MessageFormatter.arrayFormat("Val1={}, Val2={", ia);
        assertEquals("Val1=1, Val2={", ft.getMessage());
        assertEquals(t, ft.getThrowable());

        ft = MessageFormatter.arrayFormat("Value {} is smaller than {} and {}.", ia);
        assertEquals("Value 1 is smaller than 2 and 3.", ft.getMessage());
        assertEquals(t, ft.getThrowable());

        ft = MessageFormatter.arrayFormat("{}{}{}{}", ia);
        assertEquals("123java.lang.Throwable", ft.getMessage());
        assertNull(ft.getThrowable());
    }
}
