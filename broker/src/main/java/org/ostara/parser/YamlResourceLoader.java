package org.ostara.parser;

import java.util.Properties;
import org.yaml.snakeyaml.Yaml;

public class YamlResourceLoader implements ResourceLoader {
    private static final Yaml YAML = new Yaml();

    @Override
    public Properties load(String file) throws Exception {
        return YAML.loadAs(file, Properties.class);
    }
}
