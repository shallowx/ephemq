package org.ostara.client;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class Extras implements Iterable<Map.Entry<String, String>>{
    private final Map<String, String> extras;

    public Extras() {
        this(null);
    }

    public Extras(Map<String, String> extras) {
        this.extras = extras == null ? new HashMap<>() : extras;
    }

    public String getValue(String key) {
        return extras.get(key);
    }

    public String getKey(String value) {
        return extras.entrySet().stream()
                .filter(entry -> {
                    String v = entry.getValue();
                    return v.equals(value);
                }).map(Map.Entry::getKey)
                .findFirst()
                .orElse(null);
    }

    public boolean contains(String key) {
        return extras.containsKey(key);
    }

    @Override
    public Iterator<Map.Entry<String, String>> iterator() {
        return extras.entrySet().iterator();
    }

    @Override
    public String toString() {
        return "Extras{" +
                "extras=" + extras +
                '}';
    }
}
