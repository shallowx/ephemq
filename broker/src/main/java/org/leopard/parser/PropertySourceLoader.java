package org.leopard.parser;

import java.util.Properties;

public interface PropertySourceLoader {
    Properties load(String file) throws Exception;
}
