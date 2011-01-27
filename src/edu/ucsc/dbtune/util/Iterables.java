package edu.ucsc.dbtune.util;

import edu.ucsc.dbtune.core.DBIndex;
import edu.ucsc.dbtune.core.metadata.PGReifiedTypes;

import java.util.Collection;
import java.util.List;

/**
 * A collection of utility methods that will help us interact with
 * iterable objects.
 *
 * @author huascar.sanchez@gmail.com (Huascar A. Sanchez)
 */
public class Iterables {
    private Iterables(){}


    public static int count(final Iterable<?> itr){
        int counter = 0;
        for(Object each : itr){
            if(each == null) continue;
            ++counter;
        }
        return counter;
    }


    public static <T> void copy(List<? super T> to, Iterable<? extends T> from) {
        copy((Collection<? super T>)to, from);
    }

    public static <T> void copy(Collection<? super T> to, Iterable<? extends T> from){
        for(T each : from){
            to.add(each);
        }
    }

    public static <T> Collection<T> asCollection(Iterable<? extends T> target){
        List<T> result = Instances.newList();
        for (T obj : target){
            result.add(obj);
        }
        return result;
    }

    public static <I> Iterable<I> asIterable(Iterable<? extends I> target){
        final List<I> result = Instances.newList();
        for(I each : target){
           result.add(each);
        }
        return result;
    }
}
