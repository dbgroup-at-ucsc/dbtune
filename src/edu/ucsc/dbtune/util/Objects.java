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

import java.lang.reflect.Array;
import java.lang.reflect.GenericArrayType;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import java.util.Arrays;

/**
 * @author huascar.sanchez@gmail.com (Huascar A. Sanchez)
 */
public class Objects {
    private Objects(){}
    
    /**
     * check if two objects are equal.
     * @param o1
     *      first object.
     * @param o2
     *      second object.
     * @return
     *      {@code true} if equal. {@code false} otherwise.
     */
    public static boolean equals(Object o1, Object o2) {
        return o1 == o2 || !(o1 == null || o2 == null) && o1.equals(o2);
    }

    /**
     * generates a hashCode using the {@link Arrays} object.
     * @param objects
     *      objects to be used in the hashCode generation.
     * @return
     *      hashcode.
     */
    public static int hashCode(Object... objects){
        return Arrays.hashCode(objects);
    }

    /**
     * another alternative to explicit casting of objects when you know in advance
     * the class that you wanted to cast to... let me repeat again, this is a good method
     * when u know the class to which u want to downcast.
     * @param me
     *      object to be cast.
     * @param to
     *      cast to this class.
     * @param <T>
     *      type bound.
     * @param <E>
     *      type bound.
     * @return
     *      E or cast object.
     */
    @SuppressWarnings({"RedundantTypeArguments"})
    public static <T, E extends T> E cast(T me, Class<? extends E> to){
        return !to.isPrimitive() ? to.cast(me) : Objects.<E>as(me);
    }

    @SuppressWarnings({"unchecked"})
    public static <T> T as(Object obj){
        return (T) obj;
    }

    @SuppressWarnings({"unchecked"})
    public static <T> Class<T> discoverClass(T instance){
        return as(instance.getClass());
    }

    /**
     * check if there is a bond between an owner class (the one who defines
     * the members that can be accepter by the club) and an applicant object.
     * @param owner
     *      club's owner.
     * @param applicant
     *      applicant class who wishes to be part of the club
     *      which has allowed the owner to run a background check
     *      on it..
     * @return
     *      {@code true} if both classes are in the same club.
     */
    public static boolean inTheClub(Class<?> owner, Class<?> applicant){
        return ((owner.isAssignableFrom(applicant)) || (owner == applicant) || (applicant.isInstance(owner)));
    }

    /**
     * discover the raw type (class<?>) of type.
     * @param type
     *      type to be examined.
     * @return
     *      raw type in the form of a class.
     */
    public static Class<?> raw(Type type){
        if (type instanceof Class<?>) {
            // type is a normal class.
            return (Class<?>) type;

        } else if (type instanceof ParameterizedType) {
            final ParameterizedType parameterizedType = (ParameterizedType) type;

            // Neal isn't sure why getRawType() returns Type instead of Class, but suspects some pathological case related
            // to nested classes exists.
            final Type rawType = parameterizedType.getRawType();
            if(!(rawType instanceof Class)){
              throw new ClassCastException();
            }

            return (Class<?>) rawType;
        } else if (type instanceof GenericArrayType) {
            final Type componentType = ((GenericArrayType)type).getGenericComponentType();
            return Array.newInstance(raw(componentType), 0).getClass();
        } else if (type instanceof TypeVariable) {
            // we could use the variable's bounds, but that'll won't work if there are multiple.
            // having a raw type that's more general than necessary is okay
            return Object.class;
        } else {
            throw new IllegalArgumentException("Expected a Class, ParameterizedType, or "
                + "GenericArrayType, but <" + type + "> is of type " + type.getClass().getName());
        }
    }

}

