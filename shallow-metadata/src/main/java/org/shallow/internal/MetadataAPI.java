package org.shallow.internal;

import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.Promise;
import org.shallow.logging.InternalLogger;
import org.shallow.logging.InternalLoggerFactory;

import javax.naming.OperationNotSupportedException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.shallow.internal.MappedFileConstants.*;
import static org.shallow.ObjectUtil.isNull;

public class MetadataAPI {
    private static final InternalLogger logger = InternalLoggerFactory.getLogger(MetadataAPI.class);

    private final String workDirectory;

    public MetadataAPI(String workDirectory) {
        this.workDirectory = isNull(workDirectory) ? DIRECTORY : workDirectory;
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

    public String assemblesPath(String path) {
        return workDirectory + "/" + path;
    }

    public Future<String> read(Promise<String> promise, String path) {
        try {
            final String content = Files.readString(Path.of(path), UTF_8);
            promise.trySuccess(content);
        } catch (Throwable t) {
            if (logger.isErrorEnabled()) {
                logger.error(t.getMessage(), t);
            }
            promise.tryFailure(t);
        }
        return promise;
    }

    public void modify(String path, String content, Promise<Boolean> promise, Type type) {
        try {
            switch (type) {
                case APPEND -> doAdd(path, content, promise);
                case DELETE -> doDelete(path, content, promise);
                default -> throw new OperationNotSupportedException("[modify] - Not supported modify type<" + type.name() +">");
            }
        } catch (Exception e) {
            if (logger.isErrorEnabled()) {
                logger.error(e.getMessage(), e);
            }
            promise.trySuccess(false);
        }
    }

    private void doAdd(String path, String content, final Promise<Boolean> promise) {
        write2File(path, content, promise);
    }

    private void doDelete(String path, String content, final Promise<Boolean> promise) {
        write2File(path, content, promise);
    }

    private void write2File(String path, String content, final Promise<Boolean> promise) {
        Path of = Path.of(path);
        try {
            if (Files.isWritable(of)) {
                Files.writeString(of, content, UTF_8);
                promise.trySuccess(true);
            } else {
                promise.tryFailure(new RuntimeException(String.format("Failed to write to file. path:%s content:%s", path, content)));
            }
        } catch (Throwable t) {
            promise.tryFailure(t);
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
