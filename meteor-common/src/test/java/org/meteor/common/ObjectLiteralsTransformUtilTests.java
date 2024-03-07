package org.meteor.common;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Properties;

import static org.meteor.common.util.ObjectLiteralsTransformUtil.*;

public class ObjectLiteralsTransformUtilTests {

    private static final Properties prop = new Properties();

    @BeforeClass
    public static void setUp() {
        prop.setProperty("serviceId", "127.0.0.1");
        prop.setProperty("port", "8080");
        prop.setProperty("segmentSize", "1024");
        prop.setProperty("isEnabled", "true");
        prop.setProperty("sample", "0.9");
    }

    @AfterClass
    public static void clear() {
        prop.clear();
    }

    @Test
    public void testObject2String() {
        String serviceId = object2String(prop.get("serviceId"));
        Assert.assertEquals(serviceId, "127.0.0.1");
    }

    @Test
    public void testObject2Int() {
        int port = object2Int(prop.get("port"));
        Assert.assertEquals(port, 8080);
    }

    @Test
    public void testObject2Double() {
        double sample = object2Double(prop.get("sample"));
        Assert.assertEquals(sample, 0.9, 0.0001);
    }

    @Test
    public void testObject2Float() {
        float sample = object2Float(prop.get("sample"));
        Assert.assertEquals(sample, 0.9, 0.0001);
    }

    @Test
    public void testObject2Long() {
        long segmentSize = object2Long(prop.get("segmentSize"));
        Assert.assertEquals(segmentSize, 1024);
    }

    @Test
    public void testObject2Boolean() {
        boolean isEnabled = object2Boolean(prop.get("isEnabled"));
        Assert.assertTrue(isEnabled);
    }
}
