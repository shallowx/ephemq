package org.shallow.configuration;

import lombok.Getter;
import lombok.Setter;
import org.shallow.consumer.ConsumerConfig;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.NestedConfigurationProperty;
import org.springframework.context.annotation.Configuration;

@Getter
@Setter
@Configuration
@ConfigurationProperties("consumer.config")
public class ConsumerConfiguration {
    @NestedConfigurationProperty
    private ConsumerConfig consumerConfig;
}
