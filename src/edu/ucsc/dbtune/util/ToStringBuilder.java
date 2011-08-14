/*
 * ****************************************************************************
 *   Copyright 2010 University of California Santa Cruz                       *
 *                                                                            *
 *   Licensed under the Apache License, Version 2.0 (the "License");          *
 *   you may not use this file except in compliance with the License.         *
 *   You may obtain a copy of the License at                                  *
 *                                                                            *
 *       http://www.apache.org/licenses/LICENSE-2.0                           *
 *                                                                            *
 *   Unless required by applicable law or agreed to in writing, software      *
 *   distributed under the License is distributed on an "AS IS" BASIS,        *
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. *
 *   See the License for the specific language governing permissions and      *
 *   limitations under the License.                                           *
 *  ****************************************************************************
 */
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