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
package edu.ucsc.satuning.spi;

import edu.ucsc.satuning.util.Objects;
import edu.ucsc.satuning.util.ToStringBuilder;
import edu.ucsc.satuning.util.Util;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;

import static edu.ucsc.satuning.util.PreConditions.checkNotNull;
import static edu.ucsc.satuning.util.Objects.discoverClass;

/**
 * @author huascar.sanchez@gmail.com (Huascar A. Sanchez)
 */
public class Parameters {
    private Parameters(){}

    /**
     * construct an instance of a parameter type that has not name.
     * the value of {@code params} should not contain a <em>null</em>
     * instance, which is not a valid input when creating a {@link Parameter}.
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
     * instance, which is not a valid input when creating a {@link Parameter}.
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

        private final Map<Class<?>, Object> parameterMap = Util.newHashMap();

        /**
         * construct a parameter literal
         */
        protected AbstractParameter(){}

        @Override
        public abstract String getParameterName();

        @Override
        public <T> T getParameterValue(Class<T> type) {
            final Class<?> primitiveCounterpart = PRIMITIVE_TO_OBJECT.get(type);
            final boolean isPrimitiveNull       = primitiveCounterpart == null;
            if(parameterMap.containsKey(isPrimitiveNull ? type : primitiveCounterpart)){
                return Objects.cast(
                        parameterMap.get(isPrimitiveNull ? type : primitiveCounterpart),
                        type
                );
            } else {
                for(Class<?> each : parameterMap.keySet()){
                    // we'll return first match
                    if(type.isAssignableFrom(each)){
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
                parameterMap.put(key, val);
            }
        }

        @Override
        public String toString() {
            return new ToStringBuilder<AbstractParameter>(this)
                   .add(getParameterName(), parameterMap)
                   .toString();
        }
    }
}
