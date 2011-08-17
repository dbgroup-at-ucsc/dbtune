/****************************************************************************
 * Copyright 2010 Huascar A. Sanchez                                        *
 *                                                                          *
 * Licensed under the Apache License, Version 2.0 (the "License");          *
 * you may not use this file except in compliance with the License.         *
 * You may obtain a copy of the License at                                  *
 *                                                                          *
 *     http://www.apache.org/licenses/LICENSE-2.0                           *
 *                                                                          *
 * Unless required by applicable law or agreed to in writing, software      *
 * distributed under the License is distributed on an "AS IS" BASIS,        *
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. *
 * See the License for the specific language governing permissions and      *
 * limitations under the License.                                           *
 ****************************************************************************/
package edu.ucsc.dbtune.util;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;

import static edu.ucsc.dbtune.util.Objects.cast;
import static edu.ucsc.dbtune.util.Objects.raw;

/**
 * A class which will allow us to get generics info at runtime (tricks Generics Erasure)
 * if used correctly, derived from:
 * http://gafter.blogspot.com/2006/12/super-type-tokens.html
 * A typical example is when you want to retain the generic time at runtime.
 * <pre>
 * Class<Generic<Object>> a = new TypeReference<Generic<Object>>(){}.getGenericClass();
 * // above, instead of of getting Class<?> when calling Generic<Object> (e.g., b.getClass())
 * // the above code will give you the Class<Generic<Object>>.
 * </pre>
 *
 * <strong>note</strong>: this version is a little bit different than the one proposed by Gafter.
 * therefore, I will add my name as an author (more like @modifier :) )
 * @author huascar.sanchez@gmail.com (Huascar A. Sanchez)
 */
public abstract class TypeReference<T> {
    private final Type type;

    /**
     * construct a TypeReference object.
     */
    protected TypeReference() {
        final Type superclass = noMissingTypeParam(getClass().getGenericSuperclass());
        type = cast(superclass, ParameterizedType.class).getActualTypeArguments()[0];
     }

    @SuppressWarnings("rawtypes")
    @Override
    public boolean equals(Object o) {
        return (o instanceof TypeReference) && Objects.equals(this.getType(), ((TypeReference) o).getType());
    }


    @SuppressWarnings({"unchecked"})
    public Class<T> getGenericClass(){
        final boolean   isClassInstance = getType() instanceof Class<?>;
        final Class<?>  rawType         = isClassInstance ? (Class<?>) getType() : raw(getType());
        return (Class<T>) rawType;
    }

    public Type getType() {
      return this.type;
    }

    public int hashCode() {
      return getType().hashCode();
    }


    public static Type noMissingTypeParam(Type superclass){
        if (superclass instanceof Class) {
            throw new RuntimeException("Missing type parameter.");
        }
        return superclass;
    }

    @Override
    public String toString() {
        return getType().toString();
    }
}
