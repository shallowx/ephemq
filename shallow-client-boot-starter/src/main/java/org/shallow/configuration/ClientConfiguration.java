package org.shallow.configuration;

import lombok.Getter;
import lombok.Setter;
import org.shallow.ClientConfig;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.NestedConfigurationProperty;
import org.springframework.context.annotation.Configuration;

@Getter
@Setter
@Configuration
@ConfigurationProperties("client.config")
public class ClientConfiguration {
    @NestedConfigurationProperty
    private ClientConfig clientConfig;
}
