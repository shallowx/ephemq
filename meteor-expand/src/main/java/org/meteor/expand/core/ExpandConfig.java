package org.meteor.expand.core;

public record ExpandConfig(LocalFileStorageConfig localFileStorageConfig,
                           BookkeeperStorageConfig bookkeeperStorageConfig) {
}
