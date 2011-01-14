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

package edu.ucsc.dbtune.spi.core;

import edu.ucsc.dbtune.util.Objects;
import edu.ucsc.dbtune.util.ToStringBuilder;
import edu.ucsc.dbtune.util.Instances;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;

import static edu.ucsc.dbtune.util.Objects.discoverClass;
import static edu.ucsc.dbtune.util.Checks.checkNotNull;

/**
 * @author huascar.sanchez@gmail.com (Huascar A. Sanchez)
 */
public class Parameters {
    private Parameters(){}

    /**
     * construct an instance of a parameter type that has not name.
     * the value of {@code params} should not contain a <em>null</em>
     * instance, which is not a valid input when creating a {@link edu.ucsc.dbtune.spi.core.Parameter}.
     * @param params
     *      parameters that will compose the parameters.
     * @return
     *      a new {@code Parameter} instance.
     */
    public static Parameter makeAnonymousParameter(Object... params){
        return makeNamedParameter("anonymous parameter", params);
    }

    /**
     * construct an instance of a parameter type that has a name.
     * the value of {@code params} should not contain a <em>null</em>
     * instance, which is not a valid input when creating a {@link edu.ucsc.dbtune.spi.core.Parameter}.
     * 
     * @param name
     *      name of the parameter.
     * @param params
     *      parameters that will compose the parameters.
     * @return
     *      a new {@code Parameter} instance.
     */
    public static Parameter makeNamedParameter(String name, Object... params){
        final Parameter param = new SingleValueParameter(name);

        for(Object each : params){
            param.setParameterValue(each, discoverClass(each));
        }
        return param;
    }

    /**
     * Default implementation of a parameter type.
     */
    private static class SingleValueParameter extends AbstractParameter {
        private final String name;
        SingleValueParameter(String name){
            this.name = checkNotNull(name);
        }

        @Override
        public String getParameterName() {
            return name;
        }
    }

    public static abstract class AbstractParameter implements Parameter {
        private static final Map<Class<?>, Class<?>> PRIMITIVE_TO_OBJECT;
        static {
            Map<Class<?>, Class<?>> primitiveToWrapper =
                new HashMap<Class<?>, Class<?>>() {{
                 put(int.class, Integer.class);
                 put(long.class, Long.class);
                 put(boolean.class, Boolean.class);
                 put(byte.class, Byte.class);
                 put(short.class, Short.class);
                 put(float.class, Float.class);
                 put(double.class, Double.class);
                put(char.class, Character.class);
             }};

            Map<Class<?>, Class<?>> counterparts = new HashMap<Class<?>, Class<?>>();
            for (Map.Entry<Class<?>, Class<?>> entry : primitiveToWrapper.entrySet()) {
                Class<?> key    = entry.getKey();
                Class<?> value  = entry.getValue();
                counterparts.put(key, value);
            }

            PRIMITIVE_TO_OBJECT = Collections.unmodifiableMap(counterparts);
        }

        private final Map<Key<?>, Object> parameterMap;

        /**
         * construct a parameter literal
         */
        protected AbstractParameter(){
            parameterMap = Instances.newLinkedHashMap();
        }

        @Override
        public abstract String getParameterName();

        @Override
        public <T> T getParameterValue(Class<T> type) {
            final Class<?> primitiveCounterpart = PRIMITIVE_TO_OBJECT.get(type);
            final boolean isPrimitiveNull       = primitiveCounterpart == null;
            final Key<T> classKey = Key.of(type);
            final Key<?> primitiveKey = Key.of(isPrimitiveNull ? type : primitiveCounterpart);
            if(parameterMap.containsKey(isPrimitiveNull ? classKey : primitiveKey)){
                return Objects.cast(
                        parameterMap.get(isPrimitiveNull ? classKey : primitiveKey),
                        type
                );
            } else {
                for(Key<?> each : parameterMap.keySet()){
                    // we'll return first match
                    if(type.isAssignableFrom(each.object)){
                        //noinspection RedundantTypeArguments
                        return Objects.<T>as(parameterMap.get(each));  // unchecked warning.
                    }
                }
                throw new NoSuchElementException(type.getSimpleName() + " element was not found");
            }
        }

        @Override
        public <T> void setParameterValue(T value, Class<T>... types) {
            final T val = checkNotNull(value);
            for(Class<T> key : types){
                parameterMap.put(Key.of(key), val);
            }
        }

        @Override
        public String toString() {
            return new ToStringBuilder<AbstractParameter>(this)
                   .add(getParameterName(), parameterMap)
                   .toString();
        }
    }

    private static class Key <I> {
        private static final int SAFE_SEED = 37;
        Class<I>   object;
        int id;
        Key(Class<I> object){
            this.object = object;
            id = (SAFE_SEED * object.hashCode() / object.toString().length());
        }

        static <T> Key<T> of(Class<T> cls){
            return new Key<T>(cls);
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(id, object);
        }

        @Override
        public boolean equals(Object o) {
            if(!(o instanceof Key)) return false;
            final Key<?> that = Objects.as(o);
            return object.equals(that.object) && id == that.id;
        }

        @Override
        public String toString() {
            return new ToStringBuilder<Key<I>>(this)
                   .add("unique id", id)
                   .add("class", object)
                   .toString();
        }
    }
}
