package org.shallow.configuration;

import lombok.Data;
import org.shallow.ClientConfig;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.NestedConfigurationProperty;

@Data
@ConfigurationProperties(prefix = "shallow.client")
public class ClientProperties {
    private String name;

    @NestedConfigurationProperty
    private ClientConfig config;
}
