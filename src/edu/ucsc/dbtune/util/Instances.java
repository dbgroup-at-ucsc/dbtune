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

import edu.ucsc.dbtune.metadata.Index;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author huascar.sanchez@gmail.com (Huascar A. Sanchez)
 */
public class Instances {
    private Instances(){}

    /**
     * Convenience method that creates a {@code Map} object. One
     * use of this function is for saving on typing, and also to ensure
     * that typesafe Lists are created.
     *
     *
     * @return a {@code Map} of keys K and values V
     */
    public static <K,V> Map<K,V> newHashMap() {
        return new HashMap<K,V>();
    }

    public static <K, V> Map<K,V> newHashMap(int size) {
        return new HashMap<K,V>(size);
    }

    public static <K, V> Map<K,V> newLinkedHashMap(){
        return new LinkedHashMap<K,V>();
    }

    public static <V> Set<V> newTreeSet(){
        return new TreeSet<V>();
    }


    public static <T> List<T> newLinkedList(){
        return new LinkedList<T>();
    }

    public static <T> AtomicReference<T> newAtomicReference(){
        return new AtomicReference<T>();
    }

    public static AtomicInteger newAtomicInteger(){
        return newAtomicInteger(0);
    }
    
    public static AtomicInteger newAtomicInteger(int value){
        return new AtomicInteger(value);
    }

    public static <T> AtomicReference<T> newAtomicReference(T instance){
        return new AtomicReference<T>(instance);
    }

    public static AtomicBoolean newFalseBoolean(){
        return new AtomicBoolean(false);
    }

    public static AtomicBoolean newTrueBoolean(){
        return new AtomicBoolean(true);
    }

    public static <T> Set<T> newHashSet(){
        return new HashSet<T>();
    }

    public static IndexBitSet newBitSet(){
        return new IndexBitSet();
    }

    public static List<Index> newIndexList(Iterable<? extends Index> indexes){
        List<Index> list = new ArrayList<Index>();

        for(Index idx : indexes){
            list.add(idx);
        }
        return list;
    }

    /**
     */
    public static List<Boolean> asList(boolean[] array)
    {
        Boolean[] bArray = new Boolean[array.length];

        for(int i = 0; i < array.length; i++)
            bArray[i] = array[i];

        return Arrays.asList(bArray);
    }

    /**
     * Convenience method that creates a {@code List} object. One
     * use of this function is for saving on typing, and also to ensure
     * that typesafe Lists are created.
     *
     *
     * @return a {@code List} of T object.
     */
    public static <T> List<T> newList(){
        return new ArrayList<T>();
    }

    public static <T> List<T> newList(int size){
        return new ArrayList<T>(size);
    }
}
