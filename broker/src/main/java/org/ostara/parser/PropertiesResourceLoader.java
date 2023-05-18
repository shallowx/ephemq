package org.ostara.parser;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;

public class PropertiesResourceLoader implements ResourceLoader {

    @Override
    public Properties load(String file) throws Exception {
        Properties properties = new Properties();
        try (InputStream in = new BufferedInputStream(new FileInputStream(file))) {
            properties.load(in);
        }
        return properties;
    }
}
