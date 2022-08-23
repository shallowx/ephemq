package org.shallow.parser;

import java.util.Properties;

public interface PropertySourceLoader {
    Properties load(String file) throws Exception;
}
