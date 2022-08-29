package org.shallow.metadata;

import org.shallow.internal.config.BrokerConfig;
import org.shallow.log.Segment;
import org.shallow.logging.InternalLogger;
import org.shallow.logging.InternalLoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.shallow.metadata.MetadataConstants.TOPICS;

public class MappedFileApi {

    private static final InternalLogger logger = InternalLoggerFactory.getLogger(MappedFileApi.class);

    private final BrokerConfig config;

    public MappedFileApi(BrokerConfig config) {
        this.config = config;
    }

    public void start() throws Exception {
        checkWorkDirectoryIfNotExistsAndNew();
    }

    private void checkWorkDirectoryIfNotExistsAndNew() throws Exception {
        String defaultWorkDirectory = config.getWorkDirectory();

        Path workDirectoryPath = Path.of(defaultWorkDirectory);
        Path topicsPath = assemblePath(TOPICS);

        if (Files.notExists(workDirectoryPath)) {
            Files.createDirectory(workDirectoryPath);

            Files.createFile(topicsPath);

            return;
        }

        if (Files.notExists(topicsPath)) {
            Files.createFile(topicsPath);
        }
    }

    private Path assemblePath(String path) {
        return Path.of(config.getWorkDirectory() + path);
    }

    public void write(String content, String path) throws IOException {
        Files.writeString(assemblePath(path), content, StandardCharsets.UTF_8);
    }

    public String read(String path) throws IOException {
        Path filePath = assemblePath(path);
        return Files.readString(filePath, StandardCharsets.UTF_8);
    }
}
