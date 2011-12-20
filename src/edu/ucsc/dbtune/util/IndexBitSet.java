package edu.ucsc.dbtune.util;

import java.util.BitSet;
import java.util.Collection;
import java.util.Iterator;
import java.util.Set;

/**
 * This class implements the {@link Set} interface, backed by a bit array (actually a {@link BitSet} 
 * instance). This class offers constant time performance for the basic operations (add, remove, 
 * contains and size).
 * <p>
 * Iterating over this set requires time proportional to the sum of the number of set bits.
 * <p>
 * <strong>Important</strong>: For efficiency reasons, all the optional operations from {@link Set} 
 * return true regardless of whether or not the set changed as the result of the operation.
 *
 * @param <E>
 *      an implementation of the {@link Identifiable} class.
 * @author Karl Schnaitter
 * @author Ivo Jimenez
 */
public class IndexBitSet<E extends Identifiable> implements Set<E>
{
    private static final BitSet t = new BitSet();
    private BitSet bitSet;
    
    /**
     * Constructs an empty configuration.
     */
    public IndexBitSet()
    {
        bitSet = new BitSet();
    }

    /**
     * Constructs a copy of the given configuration.
     *
     * @param ibs
     *      the bitSet being copied
     */
    public IndexBitSet(IndexBitSet<E> ibs)
    {
        bitSet = (BitSet) ibs.bitSet.clone();
    }

    /**
     * Constructs a set containing the elements in the given collection.
     *
     * @param other
     *      a collection of objects
     */
    public IndexBitSet(Collection<E> other)
    {
        bitSet = new BitSet();

        for (E e : other)
            add(e);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <T> T[] toArray(T[] array)
    {
        throw new RuntimeException("not yet");
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isEmpty()
    {
        return bitSet.isEmpty();
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public Identifiable[] toArray()
    {
        throw new RuntimeException("not yet");
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void clear()
    {
        bitSet.clear();
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public boolean add(E o)
    {
        bitSet.set(o.getId());
        return true;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public int size()
    {
        return bitSet.length();
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public boolean contains(Object o)
    {
        if (!(o instanceof Identifiable))
            throw new RuntimeException("Not of type Identifiable");

        return bitSet.get(((Identifiable) o).getId());
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public boolean remove(Object o)
    {
        if (!(o instanceof Identifiable))
            throw new RuntimeException("not of type Identifiable");

        bitSet.clear(((Identifiable) o).getId());
        return true;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public boolean removeAll(Collection<?> other)
    {
        if (other instanceof IndexBitSet<?>)
            bitSet.andNot(((IndexBitSet<?>) other).bitSet);
        else
            throw new RuntimeException("not of type IndexBitSet");

        return true;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public boolean retainAll(Collection<?> other)
    {
        if (other instanceof IndexBitSet<?>)
            bitSet.and(((IndexBitSet<?>) other).bitSet);
        else
            throw new RuntimeException("not of type IndexBitSet");

        return true;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public boolean addAll(Collection<? extends E> other)
    {
        if (other instanceof IndexBitSet<?>)
            bitSet.or(((IndexBitSet<?>) other).bitSet);
        else
            throw new RuntimeException("not of type IndexBitSet");

        return true;
    }
    
    /**
     * {@inheritDoc}
     */
    public boolean containsAll(Collection<?> other)
    {
        if (!(other instanceof IndexBitSet<?>))
            throw new RuntimeException("not of type IndexBitSet");

        IndexBitSet<?> o = (IndexBitSet<?>) other;

        synchronized (t) {
            t.clear();
            t.or(o.bitSet);
            t.and(bitSet);
            return t.equals(o.bitSet);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean equals(Object other)
    {
        if (this == other)
            return true;
    
        if (!(other instanceof IndexBitSet<?>))
            return false;
    
        IndexBitSet<?> o = (IndexBitSet<?>) other;

        return bitSet.equals(o.bitSet);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int hashCode()
    {
        return bitSet.hashCode();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString()
    {
        return bitSet.toString();
    }

    /**
     * {@inheritDoc}
     */
    public Iterator<E> iterator()
    {
        throw new RuntimeException("not yet");
    }
}
