package org.meteor.logback;

import ch.qos.logback.classic.joran.JoranConfigurator;
import ch.qos.logback.core.Context;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.ILoggerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LogbackConfigurationTest {
    /**
     * The name of the SLF4J logger to be used for testing logback configuration.
     */
    private static final String LOGGER = "Slf4jTestLogger";
    /**
     * Directory path to the test resources.
     */
    private static final String DIR = "src/test/resources";

    /**
     * Initializes the logging configuration for the test.
     * This method is executed before each test method to configure the logger using a specified logback configuration file.
     *
     * @throws Exception if an error occurs during the configuration of the logger
     */
    @Before
    public void init() throws Exception {
        ILoggerFactory factory = LoggerFactory.getILoggerFactory();
        JoranConfigurator configurator = new JoranConfigurator();
        configurator.setContext((Context) factory);

        URL resource = LogbackConfigurationTest.class.getClassLoader().getResource("logback-test.xml");
        if (resource == null) {
            throw new RuntimeException("The logback file not found");
        }

        configurator.doConfigure(resource);
    }

    /**
     * Tests the SLF4J logging functionality by creating log entries of various levels
     * and asserting their presence in the log output.
     *
     * @throws Exception if an error occurs during the test execution
     */
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

    /**
     * Reads the contents of the file located at "src/test/resources/logback-test.xml" into a string.
     *
     * @return the contents of the file as a string
     * @throws IOException if an I/O error occurs reading from the file
     */
    private String readFile() throws IOException {
        StringBuilder sb = new StringBuilder();
        FileInputStream in = new FileInputStream(DIR + "/logback-test.xml");
        BufferedReader reader = new BufferedReader(new InputStreamReader(in));
        String line = reader.readLine();
        while (line != null) {
            sb.append(line).append("\r\n");
            line = reader.readLine();
        }
        reader.close();
        in.close();
        return sb.toString();
    }

    /**
     * Cleanup method executed after each test method.
     */
    @After
    public void clear() {
        // do nothing
    }
}
