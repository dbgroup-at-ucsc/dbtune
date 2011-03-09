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
package edu.ucsc.satuning.util;

import java.util.Arrays;

/**
 * @author huascar.sanchez@gmail.com (Huascar A. Sanchez)
 */
public class Objects {
    private Objects(){}

    /**
     *
     * @param o
     * @return hash code
     */
    public static int hashCode(Object... o){
        return Arrays.hashCode(o);
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

}
