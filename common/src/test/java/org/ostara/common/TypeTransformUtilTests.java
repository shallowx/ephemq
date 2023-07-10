package org.ostara.common;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.ostara.common.util.TypeTransformUtils;

import java.util.Properties;

public class TypeTransformUtilTests {

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
        String serviceId = TypeTransformUtils.object2String(prop.get("serviceId"));
        Assert.assertEquals(serviceId, "127.0.0.1");
    }

    @Test
    public void testObject2Int() {
        int port = TypeTransformUtils.object2Int(prop.get("port"));
        Assert.assertEquals(port, 8080);
    }

    @Test
    public void testObject2Double() {
        double sample = TypeTransformUtils.object2Double(prop.get("sample"));
        Assert.assertEquals(sample, 0.9,0.0001);
    }
    @Test
    public void testObject2Float() {
        float sample = TypeTransformUtils.object2Float(prop.get("sample"));
        Assert.assertEquals(sample, 0.9,0.0001);
    }

    @Test
    public void testObject2Long() {
        long segmentSize = TypeTransformUtils.object2Long(prop.get("segmentSize"));
        Assert.assertEquals(segmentSize, 1024);
    }

    @Test
    public void testObject2Boolean() {
        boolean isEnabled = TypeTransformUtils.object2Boolean(prop.get("isEnabled"));
        Assert.assertTrue(isEnabled);
    }
}
