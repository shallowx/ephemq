package org.shallow.api;

import io.netty.util.concurrent.*;
import org.shallow.internal.MetadataConfig;
import org.shallow.logging.InternalLogger;
import org.shallow.logging.InternalLoggerFactory;

import javax.naming.OperationNotSupportedException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.shallow.api.MappedFileConstants.*;
import static org.shallow.util.JsonUtil.json2Object;
import static org.shallow.util.ObjectUtil.isNotNull;
import static org.shallow.util.ObjectUtil.isNull;

public class MappedFileAPI {
    private static final InternalLogger logger = InternalLoggerFactory.getLogger(MappedFileAPI.class);

    private final String workDirectory;
    private final EventExecutor taskExecutor;
    private final MetadataConfig config;

    public MappedFileAPI(String workDirectory, EventExecutorGroup group, MetadataConfig config) {
        this.workDirectory = isNull(workDirectory) ? DIRECTORY : workDirectory;
        this.taskExecutor = group.next();
        this.config = config;
    }

    public void start() throws IOException {
        if (notExists()) {
            newDirectory();
        }

        final String topicDirectory = assemblesPath(TOPICS);
        if (notExists(topicDirectory)) {
            newFile(topicDirectory);
        }

        final String clusterDirectory = assemblesPath(CLUSTERS);
        if (notExists(clusterDirectory)) {
            newFile(clusterDirectory);
        }
    }

    private String assemblesPath(String path) {
        return workDirectory + "/" + path;
    }

    public void modify(String path, String content, Type type) {
        try {
            path = assemblesPath(path);
            switch (type) {
                case APPEND -> append(path, content);
                case DELETE -> delete(path, content);
                default -> throw new OperationNotSupportedException("[Modify] - Not supported modify type<" + type.name() +">");
            }
        } catch (Throwable t) {
            if (logger.isErrorEnabled()) {
                logger.error(t.getMessage(), t);
            }
        }
    }

    private void append(String path, String content) {
        Promise<Void> promise = taskExecutor.newPromise();
        promise.addListener((GenericFutureListener<Future<Void>>) f -> {
            if (!f.isSuccess()) {
                taskExecutor.schedule(() -> append(path, content), config.getWriteFileScheduleDelayMs(), TimeUnit.MILLISECONDS);
            }
        });

        write2File(path, content, promise);
    }

    private void delete(String path, String content) {
        Promise<Void> promise = taskExecutor.newPromise();
        promise.addListener((GenericFutureListener<Future<Void>>) f -> {
            if (!f.isSuccess()) {
                taskExecutor.schedule(() -> delete(path, content), config.getWriteFileScheduleDelayMs(), TimeUnit.MILLISECONDS);
            }
        });

        write2File(path, content, promise);
    }

    private void write2File(String path, String content, Promise<Void> promise) {
        Path of = Path.of(path);
        try {
            if (Files.isWritable(of)) {
                Files.writeString(of, content, UTF_8);
            }
            promise.trySuccess(null);
        } catch (Throwable t) {
            if (logger.isErrorEnabled()) {
                logger.error(t.getMessage(), t);
            }
            promise.tryFailure(t);
        }
    }

    public String read(String path) {
        final Path of = Path.of(assemblesPath(path));
        try {
            return Files.readString(of, UTF_8);
        } catch (Throwable t) {
            throw new RuntimeException(String.format("[Read] - failed to read partition information from file<%s>, cause: %s", of, t));
        }
    }

    private boolean notExists() {
        return Files.notExists(Path.of(workDirectory));
    }

    private Path newDirectory() throws IOException {
        return Files.createDirectory(Path.of(workDirectory));
    }

    private boolean notExists(final String path) {
        return Files.notExists(Path.of(path));
    }

    private Path newFile(final String path) throws IOException {
        return Files.createFile(Path.of(path));
    }
}
