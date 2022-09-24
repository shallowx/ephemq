package org.shallow.configuration;


import org.shallow.Client;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingClass;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@EnableConfigurationProperties(value = ClientProperties.class)
public class ClientConfiguration {

    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnMissingClass
    public Client newClient() {
       return new Client(clientProperties.getName(), clientProperties.getConfig());
    }

    private final ClientProperties clientProperties;
    public ClientConfiguration(ClientProperties clientProperties) {
        this.clientProperties = clientProperties;
    }
}
