package org.leopard.parser;

import org.yaml.snakeyaml.Yaml;

import java.util.Properties;

public class YamlPropertySourceLoader implements PropertySourceLoader {

    private static final Yaml YAML = new Yaml();

    @Override
    public Properties load(String file) throws Exception {
        return YAML.loadAs(file, Properties.class);
    }
}
