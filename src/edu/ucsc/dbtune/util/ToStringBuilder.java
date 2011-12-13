package edu.ucsc.dbtune.util;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * @author huascar.sanchez@gmail.com (Huascar A. Sanchez)
 */
public class ToStringBuilder<T> {
    // Linked hash map ensures ordering.
    private final Map<String, Object> map = new LinkedHashMap<String, Object>();
    private final String name;

    public ToStringBuilder(String name) {
        this.name = name;
    }

    public ToStringBuilder(T instance){
        this(Objects.<Class<T>>as(instance.getClass()));
    }

    public ToStringBuilder(Class<T> type) {
        this(type.getSimpleName());
    }

    public ToStringBuilder<T> add(String name, Object value) {
        if (map.put(name, value) != null) {
            throw new RuntimeException("Duplicate names: " + name);
        }
        return this;
    }

    @Override
    public String toString() {
        return name + map.toString().replace('{', '[').replace('}', ']');
    }
}
