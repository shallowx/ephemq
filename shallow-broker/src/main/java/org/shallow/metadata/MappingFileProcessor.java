package org.shallow.metadata;

import org.shallow.internal.config.BrokerConfig;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static java.nio.charset.StandardCharsets.UTF_8;

public class MappingFileProcessor {

    private final BrokerConfig config;
    public Path path;

    public MappingFileProcessor(BrokerConfig config) {
        this.config = config;
    }

    public void start() throws Exception {
        String workDirectory = config.getWorkDirectory();
        if (workDirectory == null || workDirectory.length() == 0) {
            throw new IllegalArgumentException("Metadata work directory cannot be empty");
        }

        File workFile = new File(workDirectory);
        if (!workFile.exists()) {
            Files.createDirectories(Path.of(workDirectory));
        }

        String file = workDirectory + File.separatorChar + MetadataConstants.TOPICS;
        File dataFile = new File(file);
        this.path = Path.of(file);
        if (!dataFile.exists()) {
            Files.createFile(path);
        }
    }

    public void write(String content) throws IOException {
        Files.writeString(path, content, UTF_8);
    }

    public String read() throws IOException {
        if (path == null) {
            return null;
        }
        return Files.readString(path, UTF_8);
    }
}
