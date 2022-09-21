package org.shallow.configuration;

import lombok.Getter;
import lombok.Setter;
import org.shallow.producer.ProducerConfig;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.NestedConfigurationProperty;
import org.springframework.context.annotation.Configuration;

@Getter
@Setter
@Configuration
@ConfigurationProperties("producer.config")
public class ProducerConfiguration {

    @NestedConfigurationProperty
    private ProducerConfig producerConfig;
}
