package org.ephemq.common.util;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Properties;

import static org.ephemq.common.util.ObjectLiteralsTransformUtil.*;

public class ObjectLiteralsTransformUtilTest {

    /**
     * Holds configuration properties for tests.
     * This is initialized in the setUp method and cleared in the clear method.
     */
    private static final Properties prop = new Properties();

    /**
     * Sets up the initial properties required for testing by populating the 'prop' object
     * with key-value pairs. This method is executed once before any test methods in the class.
     * <p>
     * Keys and default values set:
     * - serviceId: 127.0.0.1
     * - port: 8080
     * - segmentSize: 1024
     * - isEnabled: true
     * - sample: 0.9
     */
    @BeforeClass
    public static void setUp() {
        prop.setProperty("serviceId", "127.0.0.1");
        prop.setProperty("port", "8080");
        prop.setProperty("segmentSize", "1024");
        prop.setProperty("isEnabled", "true");
        prop.setProperty("sample", "0.9");
    }

    /**
     * Clears the properties object used in tests.
     * This method is annotated with @AfterClass, indicating that it runs once
     * after all tests in the class have been executed.
     */
    @AfterClass
    public static void clear() {
        prop.clear();
    }

    /**
     * Tests the object2String method to ensure it converts the property with key "serviceId" to its corresponding string value.
     * This test verifies that the conversion of the "serviceId" property value to a String object works as expected.
     */
    @Test
    public void testObject2String() {
        String serviceId = object2String(prop.get("serviceId"));
        Assert.assertEquals("127.0.0.1", serviceId);
    }

    /**
     * Tests the method `object2Int(Object v)` of the `ObjectLiteralsTransformUtil` class.
     * Converts the "port" property from the `prop` properties object to an integer.
     * Asserts that the converted integer value equals 8080.
     */
    @Test
    public void testObject2Int() {
        int port = object2Int(prop.get("port"));
        Assert.assertEquals(8080, port);
    }

    /**
     * Tests the {@code object2Double} method to ensure it correctly converts an object to a double.
     * This particular test verifies that the method accurately converts a property value of "0.9"
     * to its double representation.
     */
    @Test
    public void testObject2Double() {
        double sample = object2Double(prop.get("sample"));
        Assert.assertEquals(0.9, sample, 0.0001);
    }

    /**
     * Tests the object2Float method to ensure it correctly converts the property "sample" to a float.
     * <p>
     * The method retrieves the "sample" property from the prop Properties object,
     * converts it to a float using object2Float, and asserts that the converted value
     * equals 0.9 within a tolerance of 0.0001.
     *
     * @throws AssertionError if the converted float does not equal 0.9 within the specified tolerance
     */
    @Test
    public void testObject2Float() {
        float sample = object2Float(prop.get("sample"));
        Assert.assertEquals(0.9, sample, 0.0001);
    }

    /**
     * Tests the conversion of an object to a long using the object2Long method.
     * The test retrieves the string value associated with the key "segmentSize" from the properties,
     * converts it to a long, and asserts that the resulting value is 1024.
     */
    @Test
    public void testObject2Long() {
        long segmentSize = object2Long(prop.get("segmentSize"));
        Assert.assertEquals(1024, segmentSize);
    }

    /**
     * Tests the conversion of a property value to a boolean.
     * The property "isEnabled" is retrieved from the properties map and converted
     * to a boolean using the object2Boolean method.
     * Asserts that the converted boolean value is true, indicating that the
     * property "isEnabled" was successfully converted to a boolean and its value is true.
     */
    @Test
    public void testObject2Boolean() {
        boolean isEnabled = object2Boolean(prop.get("isEnabled"));
        Assert.assertTrue(isEnabled);
    }
}
