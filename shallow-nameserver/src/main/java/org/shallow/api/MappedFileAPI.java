package org.shallow.api;

import io.netty.util.concurrent.Promise;
import org.shallow.logging.InternalLogger;
import org.shallow.logging.InternalLoggerFactory;

import javax.naming.OperationNotSupportedException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.shallow.api.MappedFileConstants.*;
import static org.shallow.util.JsonUtil.json2Object;
import static org.shallow.util.ObjectUtil.isNotNull;
import static org.shallow.util.ObjectUtil.isNull;

public class MappedFileAPI {
    private static final InternalLogger logger = InternalLoggerFactory.getLogger(MappedFileAPI.class);

    private final String workDirectory;

    public MappedFileAPI(String workDirectory) {
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

    public void modify(String path, String content, Type type, Promise<Boolean> modifyPromise) {
        try {
            path = assemblesPath(path);
            switch (type) {
                case APPEND -> append(path, content, modifyPromise);
                case DELETE -> delete(path, content, modifyPromise);
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

    private void append(String path, String content, Promise<Boolean> modifyPromise) {
        write2File(path, content, modifyPromise);
    }

    private void delete(String path, String content, Promise<Boolean> modifyPromise) {
        write2File(path, content, modifyPromise);
    }

    private void write2File(String path, String content, Promise<Boolean> modifyPromise) {
        Path of = Path.of(path);
        try {
            if (Files.isWritable(of)) {
                Files.writeString(of, content, UTF_8);
                modifyPromise.trySuccess(true);
            } else {
                modifyPromise.tryFailure(new RuntimeException(String.format("[Write2File] - failed to write to file<%s>, content=%s, try again later", of, content)));
            }
        } catch (Throwable t) {
            if (logger.isErrorEnabled()) {
                logger.error(t.getMessage(), t);
            }
            modifyPromise.tryFailure(t);
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
