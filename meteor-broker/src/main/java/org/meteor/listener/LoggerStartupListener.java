package org.meteor.listener;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.spi.LoggerContextListener;
import ch.qos.logback.core.Context;
import ch.qos.logback.core.spi.ContextAwareBase;
import ch.qos.logback.core.spi.LifeCycle;

/**
 * LoggerStartupListener is responsible for configuring and starting the logging
 * context for the application. It listens to context and level change events,
 * and ensures logging starts with appropriate configurations.
 */
public class LoggerStartupListener extends ContextAwareBase implements LoggerContextListener, LifeCycle {
    /**
     * The default directory where log files will be stored if no other directory
     * is specified via the system property "log.dir". This directory is used
     * as the fallback location for logging purposes.
     */
    private static final String DEFAULT_LOG_DIR = "/tmp/meteor";
    /**
     * Indicates whether the logging context has been started.
     * This flag is used to ensure that the logging startup process
     * is only executed once.
     */
    private volatile boolean started = false;

    /**
     * Initializes a new instance of the LoggerStartupListener class.
     * This listener is responsible for configuring and starting the logging
     * context for the application, ensuring logging starts with appropriate
     * configurations and handling context and level change events.
     */
    public LoggerStartupListener() {
    }

    /**
     * Indicates whether this listener is resistant to reset events.
     *
     * @return false as this listener is not reset resistant.
     */
    @Override
    public boolean isResetResistant() {
        return false;
    }

    /**
     * Called when the logger context is started. This method is used to perform
     * any necessary initialization required when the logging system is initialized.
     *
     * @param loggerContext the {@link LoggerContext} that is being started
     */
    @Override
    public void onStart(LoggerContext loggerContext) {
    }

    /**
     * Handles the reset event for the logging context.
     * This method is called when the LoggerContext is reset.
     *
     * @param loggerContext the logging context to be reset
     */
    @Override
    public void onReset(LoggerContext loggerContext) {
    }

    /**
     * This method is invoked when the logger context is stopped.
     * It performs any necessary cleanup or shutdown operations.
     *
     * @param loggerContext the logger context being stopped
     */
    @Override
    public void onStop(LoggerContext loggerContext) {
    }

    /**
     * This method is triggered when the logging level of a logger changes.
     *
     * @param logger the logger whose logging level has changed
     * @param level  the new logging level
     */
    @Override
    public void onLevelChange(Logger logger, Level level) {
    }

    /**
     * Starts the LoggerStartupListener if it has not already been started.
     * Sets the logging directory from the system property "log.dir",
     * or defaults to "/tmp/meteor" if the property is not set.
     * The directory path is then stored in the context's properties under the key "LOG_DIR".
     */
    @Override
    public synchronized void start() {
        if (started) {
            return;
        }

        String dir = System.getProperty("log.dir");
        if (dir == null || dir.isEmpty()) {
            dir = DEFAULT_LOG_DIR;
        }

        Context crx = getContext();
        crx.putProperty("LOG_DIR", dir);
        started = true;
    }

    /**
     * Stops the logging context and performs any necessary cleanup. This method changes the
     * state of the listener to indicate that it has stopped. Once stopped, the listener can
     * be restarted by calling the start() method.
     */
    @Override
    public void stop() {
    }

    /**
     * Checks if the LoggerStartupListener has been started.
     *
     * @return true if this listener has been started, false otherwise.
     */
    @Override
    public boolean isStarted() {
        return started;
    }
}
