package org.meteor.logback;

import ch.qos.logback.classic.joran.JoranConfigurator;
import ch.qos.logback.core.Context;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.ILoggerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;

public class LogbackConfigurationTests {
    private static final String LOGGER = "Slf4jTestLogger";
    private static final String DIR = "src/test/resources";

    @Before
    public void init() throws Exception {
        ILoggerFactory factory = LoggerFactory.getILoggerFactory();
        JoranConfigurator configurator = new JoranConfigurator();
        configurator.setContext((Context) factory);

        URL resource = LogbackConfigurationTests.class.getClassLoader().getResource("logback-test.xml");
        if (resource == null) {
            throw new RuntimeException("The logback file not found");
        }

        configurator.doConfigure(resource);
    }

    @Test
    public void testSlf4j() throws Exception {
        Logger oldLogger = LoggerFactory.getLogger(LOGGER);
        Logger newLogger = LoggerFactory.getLogger(LOGGER);

        Assert.assertEquals(oldLogger.getName(), newLogger.getName());

        newLogger.info("test info");
        newLogger.warn("test warn");
        newLogger.debug("test debug");
        newLogger.error("test error");

        newLogger.info("info {}", "test");
        newLogger.warn("warn {}", "test");
        newLogger.debug("debug {}", "test");
        newLogger.error("error {}", "test");

        String content = readFile();
        Assert.assertTrue(content.contains("INFO"));
        Assert.assertTrue(content.contains("DEBUG"));
    }

    private String readFile() throws IOException {
        StringBuilder sb = new StringBuilder();
        FileInputStream in = new FileInputStream(DIR + "/logback-test.xml");
        BufferedReader reader = new BufferedReader(new InputStreamReader(in));
        String line = reader.readLine();
        while (line != null) {
            sb.append(line)
                    .append("\r\n");
            line = reader.readLine();
        }
        reader.close();
        in.close();
        return sb.toString();
    }

    @After
    public void clear() {
        // do nothing
    }
}
