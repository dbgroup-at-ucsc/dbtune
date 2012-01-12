package edu.ucsc.dbtune.util;

import java.util.HashSet;

/**
 * Class that complements the {@link Sets} class from Guava.
 *
 * @author Ivo Jimenez
 */
public final class Sets
{
    /**
     * utility class.
     */
    private Sets()
    {
    }

    /**
     * create a new hash set with the given element in it.
     *
     * @param e
     *      initial element in the set
     * @param <E>
     *      type of objects that the hash set stores
     * @return
     *      a newly created hash set with the given element in it.
     */
    public static <E> HashSet<E> newHashSet(E e)
    {
        HashSet<E> h = new HashSet<E>();

        h.add(e);

        return h;
    }
}
