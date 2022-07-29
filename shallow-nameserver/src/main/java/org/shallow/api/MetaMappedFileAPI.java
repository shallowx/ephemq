package org.shallow.api;

import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.Promise;
import org.shallow.logging.InternalLogger;
import org.shallow.logging.InternalLoggerFactory;

import javax.naming.OperationNotSupportedException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.shallow.api.MappedFileConstants.*;
import static org.shallow.util.ObjectUtil.isNotNull;
import static org.shallow.util.ObjectUtil.isNull;

public class MetaMappedFileAPI {
    private static final InternalLogger logger = InternalLoggerFactory.getLogger(MetaMappedFileAPI.class);

    private final String workDirectory;

    public MetaMappedFileAPI(String workDirectory) {
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

    private String assemblesPath(String path) {
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

    public void modify(String path, String content, Type type, Promise<Boolean> modifyPromise) {
        try {
            path = assemblesPath(path);
            switch (type) {
                case APPEND -> doAdd(path, content, modifyPromise);
                case DELETE -> doDelete(path, content, modifyPromise);
                default -> throw new OperationNotSupportedException("[Modify] - Not supported modify type<" + type.name() +">");
            }
        } catch (Throwable t) {
            if (logger.isErrorEnabled()) {
                logger.error(t.getMessage(), t);
            }
            if (isNotNull(modifyPromise)) {
                modifyPromise.tryFailure(t);
            }
        }
    }

    private void doAdd(String path, String content, Promise<Boolean> modifyPromise) {
        write2File(path, content, modifyPromise);
    }

    private void doDelete(String path, String content, Promise<Boolean> modifyPromise) {
        write2File(path, content, modifyPromise);
    }

    private void write2File(String path, String content, Promise<Boolean> modifyPromise) {
        Path of = Path.of(path);
        try {
            if (Files.isWritable(of)) {
                Files.writeString(of, content, UTF_8);
                modifyPromise.trySuccess(true);
            } else {
                modifyPromise.tryFailure(new RuntimeException("[write2File] - failed to write to file, retry"));
            }
        } catch (Throwable t) {
            if (logger.isErrorEnabled()) {
                logger.error(t.getMessage(), t);
            }
            modifyPromise.tryFailure(t);
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
