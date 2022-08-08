package org.shallow.metadata;

import org.shallow.internal.config.BrokerConfig;
import org.shallow.logging.InternalLogger;
import org.shallow.logging.InternalLoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.shallow.metadata.MetadataConstants.CLUSTERS;
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
        final String defaultWorkDirectory = config.getWorkDirectory();

        final Path workDirectoryPath = Path.of(defaultWorkDirectory);
        final Path topicsPath = assemblePath(TOPICS);
        final Path clustersPath = assemblePath(CLUSTERS);

        if (Files.notExists(workDirectoryPath)) {
            Files.createDirectory(workDirectoryPath);

            Files.createFile(topicsPath);
            Files.createFile(clustersPath);

            return;
        }

        if (Files.notExists(topicsPath)) {
            Files.createFile(topicsPath);
        }

        if (Files.notExists(clustersPath)) {
            Files.createFile(clustersPath);
        }
    }

    private Path assemblePath(String path) {
        return Path.of(config.getWorkDirectory() + path);
    }

    private void write2File(String content, String path) throws IOException {
        Files.writeString(assemblePath(path), content, StandardCharsets.UTF_8);
    }
}
