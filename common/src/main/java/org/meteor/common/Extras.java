package org.meteor.common;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class Extras implements Iterable<Map.Entry<String, String>> {

    private final Map<String, String> map;

    public Extras() {
        this.map = new HashMap<>();
    }

    public Extras(Map<String, String> map) {
        this.map = map;
    }

    public boolean contains(String key) {
        return map.containsKey(key);
    }

    @Override
    public Iterator<Map.Entry<String, String>> iterator() {
        return map.entrySet().iterator();
    }
}
