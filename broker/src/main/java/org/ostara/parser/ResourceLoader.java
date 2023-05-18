package org.ostara.parser;

import java.util.Properties;

public interface ResourceLoader {
    Properties load(String file) throws Exception;
}
