package org.ephemq.common.util;

import io.netty.util.internal.ObjectUtil;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class ObjectUtilTest {

    /**
     * A static final object representing a null value.
     * <p>
     * This constant can be used in tests or utility methods to signify
     * a deliberately null value, avoiding the need to repeatedly create new
     * null object instances.
     */
    private static final Object NULL_OBJECT = null;

    /**
     * A constant representing a non-null object.
     * This object is assigned the string "Object is not null" to indicate that it is not null.
     */
    private static final Object NON_NULL_OBJECT = "Object is not null";
    /**
     * A constant representing a non-null, empty {@code String}.
     * This value is an empty string literal ("") and can be used in scenarios
     * where a non-null but empty string is required.
     */
    private static final String NON_NULL_EMPTY_STRING = "";
    /**
     * Holds a non-null string consisting of two whitespace characters.
     * The string is used in tests that require a non-null value which is not empty
     * but only contains whitespace.
     */
    private static final String NON_NULL_WHITESPACE_STRING = "  ";
    /**
     * A constant representing an empty array of Objects that is guaranteed to be non-null.
     * It can be used as a default value for methods that return an array of Objects to avoid returning null.
     */
    private static final Object[] NON_NULL_EMPTY_OBJECT_ARRAY = {};
    /**
     * A constant array of Objects which is guaranteed to be non-null and filled with a single,
     * predefined non-null Object.
     * This array is used in tests to verify behaviors where non-null arrays with pre-initialized
     * elements are required.
     */
    private static final Object[] NON_NULL_FILLED_OBJECT_ARRAY = {NON_NULL_OBJECT};
    /**
     * A static final variable representing a null CharSequence.
     * This variable is initialized by casting a NULL_OBJECT to CharSequence.
     * It is used within ObjectUtilTest class for various test scenarios involving null CharSequence checks.
     */
    private static final CharSequence NULL_CHARSEQUENCE = (CharSequence) NULL_OBJECT;
    /**
     * A {@code CharSequence} instance that is guaranteed to be non-null.
     * This field is statically initialized from {@code NON_NULL_OBJECT}
     * which is expected to be a non-null object.
     */
    private static final CharSequence NON_NULL_CHARSEQUENCE = (CharSequence) NON_NULL_OBJECT;
    /**
     * A non-null, empty CharSequence used for testing purposes.
     */
    private static final CharSequence NON_NULL_EMPTY_CHARSEQUENCE = NON_NULL_EMPTY_STRING;
    /**
     * A byte array constant that is guaranteed to be non-null and empty.
     * This can be used in test cases or utility methods to avoid null checks and represent an empty byte array.
     */
    private static final byte[] NON_NULL_EMPTY_BYTE_ARRAY = {};
    /**
     * A byte array filled with a single non-null byte value.
     * This array can be used for testing purposes where a valid non-null byte array is required.
     */
    private static final byte[] NON_NULL_FILLED_BYTE_ARRAY = {(byte) 0xa};
    /**
     * A constant representing a non-null, empty character array.
     * This can be used in tests or methods that require a non-null, empty char array
     * to avoid null checks and simplify logic where an empty array is a valid input.
     */
    private static final char[] NON_NULL_EMPTY_CHAR_ARRAY = {};
    /**
     * A constant array of characters containing a single, non-null character 'A'.
     * This array is used in various tests to verify operations and conditions
     * involving non-empty, non-null character arrays.
     */
    private static final char[] NON_NULL_FILLED_CHAR_ARRAY = {'A'};

    /**
     * A constant representing a null name value.
     */
    private static final String NULL_NAME = "IS_NULL";
    /**
     * A constant string representing a non-null name.
     * This value is used to indicate cases where a name must not be null.
     */
    private static final String NON_NULL_NAME = "NOT_NULL";
    /**
     * A constant string used in the ObjectUtilTest class representing a name
     * that is non-null but empty. This constant is utilized in various test cases
     * to verify behaviors when dealing with non-null, yet empty, string values.
     */
    private static final String NON_NULL_EMPTY_NAME = "NOT_NULL_BUT_EMPTY";

    /**
     * Constant string used in tests to represent an expected result message
     * when a method is expected to throw a NullPointerException (NPE) or
     * IllegalArgumentException (IAE).
     */
    private static final String TEST_RESULT_NULLEX_OK = "Expected a NPE/IAE";
    /**
     * Constant used to denote that the test result expected no exception to be thrown.
     */
    private static final String TEST_RESULT_NULLEX_NOK = "Expected no exception";
    /**
     * A constant String representing a test result message indicating that
     * the expected type was not found.
     */
    private static final String TEST_RESULT_EXTYPE_NOK = "Expected type not found";

    /**
     * A constant representing the integer value zero.
     * This is used in various test cases to check methods
     * that involve integer operations and require a zero value.
     */
    private static final int ZERO_INT = 0;
    /**
     * Represents a constant value for zero in type long.
     * Used for comparison, initialization, or resetting long values to zero.
     */
    private static final long ZERO_LONG = 0;
    /**
     * A constant representing the double value zero.
     * v
     * This variable is used as a standard reference to the numerical value zero in double-precision
     * floating point operations. It serves as an initial value or as a comparison benchmark in
     * various numerical tests and assertions.
     */
    private static final double ZERO_DOUBLE = 0.0d;
    /**
     * A constant representing the float value zero (0.0f).
     */
    private static final float ZERO_FLOAT = 0.0f;

    /**
     * Represents the positive integer value 1.
     * This constant is used in various test methods to verify conditions that require
     * a positive integer value.
     */
    private static final int POS_ONE_INT = 1;
    /**
     * A constant representing the positive value of one as a long.
     */
    private static final long POS_ONE_LONG = 1;
    /**
     * A constant representing the positive double value of 1.0.
     * <p>
     * This constant is used in unit tests for object utility methods within the
     * ObjectUtilTest class. It helps to simplify and standardize the tests by
     * providing a predefined double value. This is particularly useful when
     * testing methods that require positive numeric inputs.
     */
    private static final double POS_ONE_DOUBLE = 1.0d;
    /**
     * Represents a positive floating-point value of 1.0f.
     * Used for testing purposes in the ObjectUtilTest class.
     */
    private static final float POS_ONE_FLOAT = 1.0f;

    /**
     * A constant representing negative one (-1) as an integer.
     * This value is typically used for testing or placeholder purposes
     * where a negative one value is required.
     */
    private static final int NEG_ONE_INT = -1;
    /**
     * A constant representing the long value -1.
     */
    private static final long NEG_ONE_LONG = -1;
    /**
     * A constant representing the value -1.0 as a double.
     * This can be used in test cases to signify a negative one value for comparison
     * or validation purposes against actual results in double-based analyses.
     */
    private static final double NEG_ONE_DOUBLE = -1.0d;
    /**
     * A constant representing the floating-point value of negative one (-1.0).
     * Used in various test cases within the ObjectUtilTest class to validate functionality
     * that operates with or expects floating-point values.
     */
    private static final float NEG_ONE_FLOAT = -1.0f;

    /**
     * A constant string representing the name for positive number validations.
     */
    private static final String NUM_POS_NAME = "NUMBER_POSITIVE";
    /**
     * A constant string representing the name "NUMBER_ZERO".
     */
    private static final String NUM_ZERO_NAME = "NUMBER_ZERO";
    /**
     * The name attribute used to identify or describe the value when the number is negative.
     */
    private static final String NUM_NEG_NAME = "NUMBER_NEGATIVE";

    /**
     * Tests the checkNotNull method of the ObjectUtil class.
     * The test verifies that the method does not throw an exception
     * when passed a non-null object and throws a NullPointerException
     * when passed a null object.
     */
    @Test
    public void testCheckNotNull() {
        Exception actualEx = null;
        try {
            ObjectUtil.checkNotNull(NON_NULL_OBJECT, NON_NULL_NAME);
        } catch (Exception e) {
            actualEx = e;
        }
        assertNull(actualEx, TEST_RESULT_NULLEX_NOK);

        actualEx = null;
        try {
            ObjectUtil.checkNotNull(NULL_OBJECT, NULL_NAME);
        } catch (Exception e) {
            actualEx = e;
        }
        assertNotNull(actualEx, TEST_RESULT_NULLEX_OK);
        assertInstanceOf(NullPointerException.class, actualEx, TEST_RESULT_EXTYPE_NOK);
    }

    /**
     * Validates the `checkNotNullWithIAE` method of the `ObjectUtil` class.
     * <p>
     * The test first checks that no exception is thrown when a non-null object
     * and a non-null name are passed to the method. It asserts that the
     * exception is null.
     * <p>
     * Then, it verifies that an `IllegalArgumentException` is thrown when a null
     * object and a null name are passed to the method. It asserts that the
     * exception is not null and that it is an instance of `IllegalArgumentException`.
     */
    @Test
    public void testCheckNotNullWithIAE() {
        Exception actualEx = null;
        try {
            ObjectUtil.checkNotNullWithIAE(NON_NULL_OBJECT, NON_NULL_NAME);
        } catch (Exception e) {
            actualEx = e;
        }
        assertNull(actualEx, TEST_RESULT_NULLEX_NOK);

        actualEx = null;
        try {
            ObjectUtil.checkNotNullWithIAE(NULL_OBJECT, NULL_NAME);
        } catch (Exception e) {
            actualEx = e;
        }
        assertNotNull(actualEx, TEST_RESULT_NULLEX_OK);
        assertInstanceOf(IllegalArgumentException.class, actualEx, TEST_RESULT_EXTYPE_NOK);
    }

    /**
     * Tests the `checkNotNullArrayParam` method in the `ObjectUtil` class.
     * <p>
     * The method verifies that a specific element in an array is not null.
     * The test performs the following checks:
     * <p>
     * 1. Verifies that no exception is thrown when a non-null element is passed.
     * 2. Verifies that an `IllegalArgumentException` is thrown when a null element is passed.
     * <p>
     * Uses the following asserts:
     * <p>
     * - `assertNull` to ensure no exception is thrown for a non-null element.
     * - `assertNotNull` to ensure an exception is thrown for a null element.
     * - `assertTrue` to check that the exception type is `IllegalArgumentException`.
     */
    @Test
    public void testCheckNotNullArrayParam() {
        Exception actualEx = null;
        try {
            ObjectUtil.checkNotNullArrayParam(NON_NULL_OBJECT, 1, NON_NULL_NAME);
        } catch (Exception e) {
            actualEx = e;
        }
        assertNull(actualEx, TEST_RESULT_NULLEX_NOK);

        actualEx = null;
        try {
            ObjectUtil.checkNotNullArrayParam(NULL_OBJECT, 1, NULL_NAME);
        } catch (Exception e) {
            actualEx = e;
        }
        assertNotNull(actualEx, TEST_RESULT_NULLEX_OK);
        assertInstanceOf(IllegalArgumentException.class, actualEx, TEST_RESULT_EXTYPE_NOK);
    }

    /**
     * Tests the checkPositive method of the ObjectUtil class with an integer and a string name.
     * This method performs the following tests:
     * <p>
     * - Verifies that no exception is thrown when a positive integer is passed.
     * - Verifies that an IllegalArgumentException is thrown when zero is passed as an argument.
     * - Verifies that an IllegalArgumentException is thrown when a negative integer is passed as an argument.
     * <p>
     * The method uses several named constants for the test values and assertion messages.
     */
    @Test
    public void testCheckPositiveIntString() {
        Exception actualEx = null;
        try {
            ObjectUtil.checkPositive(POS_ONE_INT, NUM_POS_NAME);
        } catch (Exception e) {
            actualEx = e;
        }
        assertNull(actualEx, TEST_RESULT_NULLEX_NOK);

        actualEx = null;
        try {
            ObjectUtil.checkPositive(ZERO_INT, NUM_ZERO_NAME);
        } catch (Exception e) {
            actualEx = e;
        }
        assertNotNull(actualEx, TEST_RESULT_NULLEX_OK);
        assertTrue(actualEx instanceof IllegalArgumentException, TEST_RESULT_EXTYPE_NOK);

        actualEx = null;
        try {
            ObjectUtil.checkPositive(NEG_ONE_INT, NUM_NEG_NAME);
        } catch (Exception e) {
            actualEx = e;
        }
        assertNotNull(actualEx, TEST_RESULT_NULLEX_OK);
        assertTrue(actualEx instanceof IllegalArgumentException, TEST_RESULT_EXTYPE_NOK);
    }

    /**
     * Tests the ObjectUtil.checkPositive method with a long value and a string name.
     * <p>
     * Ensures that the method does not throw an exception when provided with a positive long value.
     * Verifies that the method throws an IllegalArgumentException when provided with a zero or negative long value.
     * <p>
     * Specifically, this test case performs the following checks:
     * 1. Asserts that no exception is thrown for a positive long value.
     * 2. Asserts that an IllegalArgumentException is thrown for a zero long value.
     * 3. Asserts that an IllegalArgumentException is thrown for a negative long value.
     */
    @Test
    public void testCheckPositiveLongString() {
        Exception actualEx = null;
        try {
            ObjectUtil.checkPositive(POS_ONE_LONG, NUM_POS_NAME);
        } catch (Exception e) {
            actualEx = e;
        }
        assertNull(actualEx, TEST_RESULT_NULLEX_NOK);

        actualEx = null;
        try {
            ObjectUtil.checkPositive(ZERO_LONG, NUM_ZERO_NAME);
        } catch (Exception e) {
            actualEx = e;
        }
        assertNotNull(actualEx, TEST_RESULT_NULLEX_OK);
        assertTrue(actualEx instanceof IllegalArgumentException, TEST_RESULT_EXTYPE_NOK);

        actualEx = null;
        try {
            ObjectUtil.checkPositive(NEG_ONE_LONG, NUM_NEG_NAME);
        } catch (Exception e) {
            actualEx = e;
        }
        assertNotNull(actualEx, TEST_RESULT_NULLEX_OK);
        assertTrue(actualEx instanceof IllegalArgumentException, TEST_RESULT_EXTYPE_NOK);
    }

    /**
     * Tests the checkPositive method in ObjectUtil for double values with a descriptive name.
     * This test covers three scenarios:
     * 1. A positive double value, expecting no exception.
     * 2. A zero double value, expecting an IllegalArgumentException.
     * 3. A negative double value, expecting an IllegalArgumentException.
     * <p>
     * The test assertions ensure that:
     * - No exception is thrown for positive values.
     * - An IllegalArgumentException is thrown for zero and negative values.
     * - The thrown exception is of type IllegalArgumentException.
     */
    @Test
    public void testCheckPositiveDoubleString() {
        Exception actualEx = null;
        try {
            ObjectUtil.checkPositive(POS_ONE_DOUBLE, NUM_POS_NAME);
        } catch (Exception e) {
            actualEx = e;
        }
        assertNull(actualEx, TEST_RESULT_NULLEX_NOK);

        actualEx = null;
        try {
            ObjectUtil.checkPositive(ZERO_DOUBLE, NUM_ZERO_NAME);
        } catch (Exception e) {
            actualEx = e;
        }
        assertNotNull(actualEx, TEST_RESULT_NULLEX_OK);
        assertTrue(actualEx instanceof IllegalArgumentException, TEST_RESULT_EXTYPE_NOK);

        actualEx = null;
        try {
            ObjectUtil.checkPositive(NEG_ONE_DOUBLE, NUM_NEG_NAME);
        } catch (Exception e) {
            actualEx = e;
        }
        assertNotNull(actualEx, TEST_RESULT_NULLEX_OK);
        assertTrue(actualEx instanceof IllegalArgumentException, TEST_RESULT_EXTYPE_NOK);
    }

    /**
     * Test method for {@link ObjectUtil#checkPositive(float, String)}.
     * <p>
     * This test method verifies that the checkPositive method works correctly when passed positive, zero,
     * and negative float values. It ensures that no exception is thrown for positive values and that an
     * IllegalArgumentException is thrown for zero and negative values.
     * <p>
     * The method conducts three checks:
     * 1. Ensures no exception is thrown when a positive float is passed.
     * 2. Ensures IllegalArgumentException is thrown when zero float is passed.
     * 3. Ensures IllegalArgumentException is thrown when a negative float is passed.
     */
    @Test
    public void testCheckPositiveFloatString() {
        Exception actualEx = null;
        try {
            ObjectUtil.checkPositive(POS_ONE_FLOAT, NUM_POS_NAME);
        } catch (Exception e) {
            actualEx = e;
        }
        assertNull(actualEx, TEST_RESULT_NULLEX_NOK);

        actualEx = null;
        try {
            ObjectUtil.checkPositive(ZERO_FLOAT, NUM_ZERO_NAME);
        } catch (Exception e) {
            actualEx = e;
        }
        assertNotNull(actualEx, TEST_RESULT_NULLEX_OK);
        assertTrue(actualEx instanceof IllegalArgumentException, TEST_RESULT_EXTYPE_NOK);

        actualEx = null;
        try {
            ObjectUtil.checkPositive(NEG_ONE_FLOAT, NUM_NEG_NAME);
        } catch (Exception e) {
            actualEx = e;
        }
        assertNotNull(actualEx, TEST_RESULT_NULLEX_OK);
        assertTrue(actualEx instanceof IllegalArgumentException, TEST_RESULT_EXTYPE_NOK);
    }

    /**
     * Tests the checkPositiveOrZero method of ObjectUtil class with integer and string parameters.
     * <p>
     * Validates that no exception is thrown when positive or zero integers are checked.
     * Expects an IllegalArgumentException when a negative integer is checked.
     * Uses JUnit assertions to verify if exceptions are thrown or not and checks the type of the exception.
     */
    @Test
    public void testCheckPositiveOrZeroIntString() {
        Exception actualEx = null;
        try {
            ObjectUtil.checkPositiveOrZero(POS_ONE_INT, NUM_POS_NAME);
        } catch (Exception e) {
            actualEx = e;
        }
        assertNull(actualEx, TEST_RESULT_NULLEX_NOK);

        actualEx = null;
        try {
            ObjectUtil.checkPositiveOrZero(ZERO_INT, NUM_ZERO_NAME);
        } catch (Exception e) {
            actualEx = e;
        }
        assertNull(actualEx, TEST_RESULT_NULLEX_NOK);

        actualEx = null;
        try {
            ObjectUtil.checkPositiveOrZero(NEG_ONE_INT, NUM_NEG_NAME);
        } catch (Exception e) {
            actualEx = e;
        }
        assertNotNull(actualEx, TEST_RESULT_NULLEX_OK);
        assertTrue(actualEx instanceof IllegalArgumentException, TEST_RESULT_EXTYPE_NOK);
    }

    /**
     * Tests the checkPositiveOrZero method from the ObjectUtil class with various long values.
     * <p>
     * This test case verifies that the checkPositiveOrZero method:
     * - Does not throw an exception when a positive or zero long value is provided.
     * - Throws an IllegalArgumentException when a negative long value is provided.
     * <p>
     * Test scenarios include:
     * - Positive long value (POS_ONE_LONG) with a name (NUM_POS_NAME)
     * - Zero long value (ZERO_LONG) with a name (NUM_ZERO_NAME)
     * - Negative long value (NEG_ONE_LONG) with a name (NUM_NEG_NAME)
     * <p>
     * Assertions:
     * - For positive and zero values, no exception should be thrown.
     * - For negative values, an IllegalArgumentException should be thrown.
     * - The type of exception for negative values must be IllegalArgumentException.
     */
    @Test
    public void testCheckPositiveOrZeroLongString() {
        Exception actualEx = null;
        try {
            ObjectUtil.checkPositiveOrZero(POS_ONE_LONG, NUM_POS_NAME);
        } catch (Exception e) {
            actualEx = e;
        }
        assertNull(actualEx, TEST_RESULT_NULLEX_NOK);

        actualEx = null;
        try {
            ObjectUtil.checkPositiveOrZero(ZERO_LONG, NUM_ZERO_NAME);
        } catch (Exception e) {
            actualEx = e;
        }
        assertNull(actualEx, TEST_RESULT_NULLEX_NOK);

        actualEx = null;
        try {
            ObjectUtil.checkPositiveOrZero(NEG_ONE_LONG, NUM_NEG_NAME);
        } catch (Exception e) {
            actualEx = e;
        }
        assertNotNull(actualEx, TEST_RESULT_NULLEX_OK);
        assertTrue(actualEx instanceof IllegalArgumentException, TEST_RESULT_EXTYPE_NOK);
    }

    /**
     * Tests the checkPositiveOrZero method of ObjectUtil class with double values.
     * <p>
     * This method validates the behavior of ObjectUtil.checkPositiveOrZero when
     * provided with different double values that are either positive, zero, or negative.
     * It verifies that no exception is thrown for positive and zero values, and an
     * IllegalArgumentException is thrown for negative values.
     * <p>
     * This test performs the following checks:
     * 1. Passes a positive double value and expects no exception.
     * 2. Passes zero as a double value and expects no exception.
     * 3. Passes a negative double value and expects an IllegalArgumentException.
     */
    @Test
    public void testCheckPositiveOrZeroDoubleString() {
        Exception actualEx = null;
        try {
            ObjectUtil.checkPositiveOrZero(POS_ONE_DOUBLE, NUM_POS_NAME);
        } catch (Exception e) {
            actualEx = e;
        }
        assertNull(actualEx, TEST_RESULT_NULLEX_NOK);

        actualEx = null;
        try {
            ObjectUtil.checkPositiveOrZero(ZERO_DOUBLE, NUM_ZERO_NAME);
        } catch (Exception e) {
            actualEx = e;
        }
        assertNull(actualEx, TEST_RESULT_NULLEX_NOK);

        actualEx = null;
        try {
            ObjectUtil.checkPositiveOrZero(NEG_ONE_DOUBLE, NUM_NEG_NAME);
        } catch (Exception e) {
            actualEx = e;
        }
        assertNotNull(actualEx, TEST_RESULT_NULLEX_OK);
        assertTrue(actualEx instanceof IllegalArgumentException, TEST_RESULT_EXTYPE_NOK);
    }

    /**
     * Tests the {@code checkPositiveOrZero} method of {@code ObjectUtil} for different float values.
     * <p>
     * This method verifies the following scenarios:
     * <ul>
     *   <li>When a positive float value is provided, no exception is thrown.</li>
     *   <li>When zero float value is provided, no exception is thrown.</li>
     *   <li>When a negative float value is provided, an {@link IllegalArgumentException} is thrown.</li>
     * </ul>
     *
     * Assertions used:
     * <ul>
     *   <li> to ensure no exception is thrown for positive or zero values.</li>
     *   <li> to ensure an exception is thrown for negative values.</li>
     *   <li> to check the type of exception thrown is {@link IllegalArgumentException}.</li>
     * </ul>
     */
    @Test
    public void testCheckPositiveOrZeroFloatString() {
        Exception actualEx = null;
        try {
            ObjectUtil.checkPositiveOrZero(POS_ONE_FLOAT, NUM_POS_NAME);
        } catch (Exception e) {
            actualEx = e;
        }
        assertNull(actualEx, TEST_RESULT_NULLEX_NOK);

        actualEx = null;
        try {
            ObjectUtil.checkPositiveOrZero(ZERO_FLOAT, NUM_ZERO_NAME);
        } catch (Exception e) {
            actualEx = e;
        }
        assertNull(actualEx, TEST_RESULT_NULLEX_NOK);

        actualEx = null;
        try {
            ObjectUtil.checkPositiveOrZero(NEG_ONE_FLOAT, NUM_NEG_NAME);
        } catch (Exception e) {
            actualEx = e;
        }
        assertNotNull(actualEx, TEST_RESULT_NULLEX_OK);
        assertTrue(actualEx instanceof IllegalArgumentException, TEST_RESULT_EXTYPE_NOK);
    }

    /**
     * Tests the checkNonEmpty method of ObjectUtil for Object arrays.
     * <p>
     * This method verifies three conditions:
     * 1. A NullPointerException is thrown when the object array passed is null.
     * 2. No exception is thrown when the object array passed is non-null and non-empty.
     * 3. An IllegalArgumentException is thrown when the object array passed is non-null but empty.
     * <p>
     * The tests use predefined class constants: NULL_OBJECT, NULL_NAME, NON_NULL_FILLED_OBJECT_ARRAY,
     * NON_NULL_NAME, NON_NULL_EMPTY_OBJECT_ARRAY, NON_NULL_EMPTY_NAME, TEST_RESULT_NULLEX_OK,
     * TEST_RESULT_NULLEX_NOK, and TEST_RESULT_EXTYPE_NOK.
     */
    @Test
    public void testCheckNonEmptyTArrayString() {
        Exception actualEx = null;

        try {
            ObjectUtil.checkNonEmpty((Object[]) NULL_OBJECT, NULL_NAME);
        } catch (Exception e) {
            actualEx = e;
        }
        assertNotNull(actualEx, TEST_RESULT_NULLEX_OK);
        assertTrue(actualEx instanceof NullPointerException, TEST_RESULT_EXTYPE_NOK);

        actualEx = null;
        try {
            ObjectUtil.checkNonEmpty(NON_NULL_FILLED_OBJECT_ARRAY, NON_NULL_NAME);
        } catch (Exception e) {
            actualEx = e;
        }
        assertNull(actualEx, TEST_RESULT_NULLEX_NOK);

        actualEx = null;
        try {
            ObjectUtil.checkNonEmpty(NON_NULL_EMPTY_OBJECT_ARRAY, NON_NULL_EMPTY_NAME);
        } catch (Exception e) {
            actualEx = e;
        }
        assertNotNull(actualEx, TEST_RESULT_NULLEX_OK);
        assertTrue(actualEx instanceof IllegalArgumentException, TEST_RESULT_EXTYPE_NOK);
    }

    /**
     * Tests the {@code checkNonEmpty} method in {@code ObjectUtil} for byte array input and a string message.
     * <p>
     * This test checks that the method correctly handles:
     * 1. A null byte array, expecting a {@code NullPointerException}.
     * 2. A non-null byte array with content, where no exception is expected.
     * 3. An empty byte array, expecting an {@code IllegalArgumentException}.
     * <p>
     * Validates that the appropriate exceptions are thrown or not thrown as expected:
     * - Asserts {@code NullPointerException} for a null byte array.
     * - Asserts no exception for a non-empty byte array.
     * - Asserts {@code IllegalArgumentException} for an empty byte array.
     */
    @Test
    public void testCheckNonEmptyByteArrayString() {
        Exception actualEx = null;

        try {
            ObjectUtil.checkNonEmpty((byte[]) NULL_OBJECT, NULL_NAME);
        } catch (Exception e) {
            actualEx = e;
        }
        assertNotNull(actualEx, TEST_RESULT_NULLEX_OK);
        assertTrue(actualEx instanceof NullPointerException, TEST_RESULT_EXTYPE_NOK);

        actualEx = null;
        try {
            ObjectUtil.checkNonEmpty(NON_NULL_FILLED_BYTE_ARRAY, NON_NULL_NAME);
        } catch (Exception e) {
            actualEx = e;
        }
        assertNull(actualEx, TEST_RESULT_NULLEX_NOK);

        actualEx = null;
        try {
            ObjectUtil.checkNonEmpty(NON_NULL_EMPTY_BYTE_ARRAY, NON_NULL_EMPTY_NAME);
        } catch (Exception e) {
            actualEx = e;
        }
        assertNotNull(actualEx, TEST_RESULT_NULLEX_OK);
        assertTrue(actualEx instanceof IllegalArgumentException, TEST_RESULT_EXTYPE_NOK);
    }

    /**
     * Tests the method ObjectUtil.checkNonEmpty(char[], String).
     * <p>
     * The test verifies the following scenarios:
     * 1. When the character array is null, a NullPointerException is thrown.
     * 2. When the character array is non-null and filled, no exception is thrown.
     * 3. When the character array is non-null but empty, an IllegalArgumentException is thrown.
     */
    @Test
    public void testCheckNonEmptyCharArrayString() {
        Exception actualEx = null;

        try {
            ObjectUtil.checkNonEmpty((char[]) NULL_OBJECT, NULL_NAME);
        } catch (Exception e) {
            actualEx = e;
        }
        assertNotNull(actualEx, TEST_RESULT_NULLEX_OK);
        assertTrue(actualEx instanceof NullPointerException, TEST_RESULT_EXTYPE_NOK);

        actualEx = null;
        try {
            ObjectUtil.checkNonEmpty(NON_NULL_FILLED_CHAR_ARRAY, NON_NULL_NAME);
        } catch (Exception e) {
            actualEx = e;
        }
        assertNull(actualEx, TEST_RESULT_NULLEX_NOK);

        actualEx = null;
        try {
            ObjectUtil.checkNonEmpty(NON_NULL_EMPTY_CHAR_ARRAY, NON_NULL_EMPTY_NAME);
        } catch (Exception e) {
            actualEx = e;
        }
        assertNotNull(actualEx, TEST_RESULT_NULLEX_OK);
        assertTrue(actualEx instanceof IllegalArgumentException, TEST_RESULT_EXTYPE_NOK);
    }

    /**
     * Tests the checkNonEmpty method with various object arrays.
     * <p>
     * This test method verifies the following scenarios for the checkNonEmpty function:
     * - A NullPointerException is thrown when a null object array is passed.
     * - No exception is thrown when a non-null object array with elements is passed.
     * - An IllegalArgumentException is thrown when a non-null empty object array is passed.
     * <p>
     * Asserts:
     * - The thrown exception is not null for null object array.
     * - The thrown exception type is NullPointerException for null object array.
     * - No exception is thrown for a non-null filled object array.
     * - The thrown exception is not null for a non-null empty object array.
     * - The thrown exception type is IllegalArgumentException for a non-null empty object array.
     */
    @Test
    public void testCheckNonEmptyTString() {
        Exception actualEx = null;
        try {
            ObjectUtil.checkNonEmpty((Object[]) NULL_OBJECT, NULL_NAME);
        } catch (Exception e) {
            actualEx = e;
        }
        assertNotNull(actualEx, TEST_RESULT_NULLEX_OK);
        assertTrue(actualEx instanceof NullPointerException, TEST_RESULT_EXTYPE_NOK);

        actualEx = null;
        try {
            ObjectUtil.checkNonEmpty(NON_NULL_FILLED_OBJECT_ARRAY, NON_NULL_NAME);
        } catch (Exception e) {
            actualEx = e;
        }
        assertNull(actualEx, TEST_RESULT_NULLEX_NOK);

        actualEx = null;
        try {
            ObjectUtil.checkNonEmpty(NON_NULL_EMPTY_OBJECT_ARRAY, NON_NULL_EMPTY_NAME);
        } catch (Exception e) {
            actualEx = e;
        }
        assertNotNull(actualEx, TEST_RESULT_NULLEX_OK);
        assertTrue(actualEx instanceof IllegalArgumentException, TEST_RESULT_EXTYPE_NOK);
    }

    /**
     * Tests the {@code checkNonEmpty} method from {@code ObjectUtil} when supplied with various
     * {@code String} values and their respective names. This test covers the following scenarios:
     *
     * <ul>
     *   <li>When the provided string value is {@code null}, a {@code NullPointerException} is expected.</li>
     *   <li>When the provided string value is a non-null, non-empty string, no exception should be thrown.</li>
     *   <li>When the provided string value is an empty string, an {@code IllegalArgumentException} is expected.</li>
     *   <li>When the provided string value is a string containing only whitespace characters, no exception should be thrown.</li>
     * </ul>
     *
     * Several assertions are used to verify the outcomes, including {@code assertNotNull}, {@code assertNull},
     * and {@code assertTrue} to ensure the correct type of exception is thrown, if any.
     */
    @Test
    public void testCheckNonEmptyStringString() {
        Exception actualEx = null;

        try {
            ObjectUtil.checkNonEmpty((String) NULL_OBJECT, NULL_NAME);
        } catch (Exception e) {
            actualEx = e;
        }
        assertNotNull(actualEx, TEST_RESULT_NULLEX_OK);
        assertTrue(actualEx instanceof NullPointerException, TEST_RESULT_EXTYPE_NOK);

        actualEx = null;
        try {
            ObjectUtil.checkNonEmpty((String) NON_NULL_OBJECT, NON_NULL_NAME);
        } catch (Exception e) {
            actualEx = e;
        }
        assertNull(actualEx, TEST_RESULT_NULLEX_NOK);

        actualEx = null;
        try {
            ObjectUtil.checkNonEmpty(NON_NULL_EMPTY_STRING, NON_NULL_EMPTY_NAME);
        } catch (Exception e) {
            actualEx = e;
        }
        assertNotNull(actualEx, TEST_RESULT_NULLEX_OK);
        assertTrue(actualEx instanceof IllegalArgumentException, TEST_RESULT_EXTYPE_NOK);

        actualEx = null;
        try {
            ObjectUtil.checkNonEmpty(NON_NULL_WHITESPACE_STRING, NON_NULL_EMPTY_NAME);
        } catch (Exception e) {
            actualEx = e;
        }
        assertNull(actualEx, TEST_RESULT_NULLEX_NOK);
    }

    /**
     * Tests the {@code checkNonEmpty} method with various {@code CharSequence} inputs to ensure that
     * it correctly validates non-empty character sequences.
     * <p>
     * Specifically, this test case:
     * <ul>
     *     <li>Validates that a {@code NullPointerException} is thrown when a null {@code CharSequence} is passed.</li>
     *     <li>Validates that no exception is thrown when a non-null, non-empty {@code CharSequence} is passed.</li>
     *     <li>Validates that an {@code IllegalArgumentException} is thrown when a non-null but empty {@code CharSequence} is passed.</li>
     *     <li>Validates that no exception is thrown when a non-null {@code CharSequence} consisting of whitespace is passed.</li>
     * </ul>
     *
     * The test checks for both the presence and type of exceptions to ensure the method behaves as expected.
     */
    @Test
    public void testCheckNonEmptyCharSequenceString() {
        Exception actualEx = null;

        try {
            ObjectUtil.checkNonEmpty(NULL_CHARSEQUENCE, NULL_NAME);
        } catch (Exception e) {
            actualEx = e;
        }
        assertNotNull(actualEx, TEST_RESULT_NULLEX_OK);
        assertTrue(actualEx instanceof NullPointerException, TEST_RESULT_EXTYPE_NOK);

        actualEx = null;
        try {
            ObjectUtil.checkNonEmpty(NON_NULL_CHARSEQUENCE, NON_NULL_NAME);
        } catch (Exception e) {
            actualEx = e;
        }
        assertNull(actualEx, TEST_RESULT_NULLEX_NOK);

        actualEx = null;
        try {
            ObjectUtil.checkNonEmpty(NON_NULL_EMPTY_CHARSEQUENCE, NON_NULL_EMPTY_NAME);
        } catch (Exception e) {
            actualEx = e;
        }
        assertNotNull(actualEx, TEST_RESULT_NULLEX_OK);
        assertTrue(actualEx instanceof IllegalArgumentException, TEST_RESULT_EXTYPE_NOK);

        actualEx = null;
        try {
            ObjectUtil.checkNonEmpty((CharSequence) NON_NULL_WHITESPACE_STRING, NON_NULL_EMPTY_NAME);
        } catch (Exception e) {
            actualEx = e;
        }
        assertNull(actualEx, TEST_RESULT_NULLEX_NOK);
    }

    /**
     * Tests the {@link ObjectUtil#checkNonEmptyAfterTrim(String, String)} method.
     * <p>
     * This test verifies the following scenarios:
     * <p>
     * 1. When the input String is null, a NullPointerException is thrown.
     * 2. When the input String is non-null and non-empty, no exception is thrown.
     * 3. When the input String is empty after trimming, an IllegalArgumentException is thrown.
     * 4. When the input String contains only whitespace characters and becomes empty after trimming, an IllegalArgumentException is thrown.
     */
    @Test
    public void testCheckNonEmptyAfterTrim() {
        Exception actualEx = null;

        try {
            ObjectUtil.checkNonEmptyAfterTrim((String) NULL_OBJECT, NULL_NAME);
        } catch (Exception e) {
            actualEx = e;
        }
        assertNotNull(actualEx, TEST_RESULT_NULLEX_OK);
        assertTrue(actualEx instanceof NullPointerException, TEST_RESULT_EXTYPE_NOK);

        actualEx = null;
        try {
            ObjectUtil.checkNonEmptyAfterTrim((String) NON_NULL_OBJECT, NON_NULL_NAME);
        } catch (Exception e) {
            actualEx = e;
        }
        assertNull(actualEx, TEST_RESULT_NULLEX_NOK);

        actualEx = null;
        try {
            ObjectUtil.checkNonEmptyAfterTrim(NON_NULL_EMPTY_STRING, NON_NULL_EMPTY_NAME);
        } catch (Exception e) {
            actualEx = e;
        }
        assertNotNull(actualEx, TEST_RESULT_NULLEX_OK);
        assertTrue(actualEx instanceof IllegalArgumentException, TEST_RESULT_EXTYPE_NOK);

        actualEx = null;
        try {
            ObjectUtil.checkNonEmptyAfterTrim(NON_NULL_WHITESPACE_STRING, NON_NULL_EMPTY_NAME);
        } catch (Exception e) {
            actualEx = e;
        }
        assertNotNull(actualEx, TEST_RESULT_NULLEX_OK);
        assertTrue(actualEx instanceof IllegalArgumentException, TEST_RESULT_EXTYPE_NOK);
    }
}
