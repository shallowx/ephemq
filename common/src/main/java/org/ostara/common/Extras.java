package org.ostara.common;

import org.jetbrains.annotations.NotNull;

import java.util.Iterator;
import java.util.Map;

public class Extras implements Iterable<Map.Entry<String, String>> {

    private Map<String, String> map;

    public Extras() {
    }

    public Extras(Map<String, String> map) {
        this.map = map;
    }

    public boolean contains(String key) {
        return map.containsKey(key);
    }

    @NotNull
    @Override
    public Iterator<Map.Entry<String, String>> iterator() {
        return null;
    }
}
