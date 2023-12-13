package org.meteor.coordinatio;

import org.junit.Assert;
import org.junit.Test;

import java.util.regex.Pattern;

public class ZookeeperTopicCoordinatorTests {
    @Test
    public void testTopicPattern() throws Exception {
        Pattern pattern = Pattern.compile("^[\\w\\-#]+$");
        Assert.assertTrue(pattern.matcher("test").matches());
        Assert.assertTrue(pattern.matcher("001").matches());
        Assert.assertTrue(pattern.matcher("100").matches());
        Assert.assertTrue(pattern.matcher("test-1").matches());
        Assert.assertTrue(pattern.matcher("test_1").matches());
        Assert.assertTrue(pattern.matcher("test-1_1").matches());
        Assert.assertTrue(pattern.matcher("test-1_1").matches());
        Assert.assertTrue(pattern.matcher("test#001").matches());
        Assert.assertTrue(pattern.matcher("test-#_001").matches());
        Assert.assertFalse(pattern.matcher("test/001").matches());
        Assert.assertFalse(pattern.matcher("test\\001").matches());
        Assert.assertFalse(pattern.matcher("test&001").matches());
        Assert.assertFalse(pattern.matcher("test&%001").matches());
        Assert.assertFalse(pattern.matcher("test&$001").matches());
        Assert.assertFalse(pattern.matcher("test&@001").matches());
        Assert.assertFalse(pattern.matcher("test 001").matches());
    }
}
