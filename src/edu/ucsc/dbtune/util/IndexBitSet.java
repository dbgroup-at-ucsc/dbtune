package edu.ucsc.dbtune.util;

import java.util.BitSet;
import java.util.Collection;
import java.util.Iterator;
import java.util.Set;

/**
 * A configuration represents a set of physical structures over tables (and/or columns) that are 
 * used to improve the performance of DML statements in a database. A configuration is typically 
 * composed of a set of indexes, but can also contain materialized views (MV), partitions, 
 * denormalizations, etc.
 *
 * <strong>Important</strong>: For efficiency reasons, all the optional operations from {@link Set} 
 * return true regardless of whether or not the set changed as the result of the operation.
 *
 * @author Karl Schnaitter
 * @author Ivo Jimenez
 */
//public class IndexBitSet<E extends Identifiable> implements Set<Identifiable>
public class IndexBitSet implements Set<Integer>
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
    public IndexBitSet(IndexBitSet ibs)
    {
        bitSet = (BitSet) ibs.bitSet.clone();
    }

    /**
     * {@inheritDoc}
     */
    public boolean contains(int id)
    {
        return contains(new Integer(id));
    }
    
    /**
     * {@inheritDoc}
     */
    public void remove(int id)
    {
        remove(new Integer(id));
    }

    /**
     * {@inheritDoc}
     */
    public Iterator<Integer> iterator()
    {
        throw new RuntimeException("not yet");
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
    public Integer[] toArray()
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
    public boolean add(Integer id)
    {
        bitSet.set(id);
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
        if (!(o instanceof Integer))
            return false;

        return bitSet.get((Integer) o);
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public boolean remove(Object o)
    {
        if (o instanceof Integer)
            bitSet.clear((Integer) o);
        else
            throw new RuntimeException("not of type Integer");

        return true;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public boolean removeAll(Collection<?> other)
    {
        if (other instanceof IndexBitSet)
            bitSet.andNot(((IndexBitSet) other).bitSet);
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
        if (other instanceof IndexBitSet)
            bitSet.and(((IndexBitSet) other).bitSet);
        else
            throw new RuntimeException("not of type IndexBitSet");

        return true;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public boolean addAll(Collection<? extends Integer> other)
    {
        if (other instanceof IndexBitSet)
            bitSet.or(((IndexBitSet) other).bitSet);
        else
            throw new RuntimeException("not of type IndexBitSet");

        return true;
    }
    
    /**
     * {@inheritDoc}
     */
    public boolean containsAll(Collection<?> other)
    {
        if (!(other instanceof IndexBitSet))
            throw new RuntimeException("not of type IndexBitSet");

        IndexBitSet o = (IndexBitSet) other;

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
    
        if (!(other instanceof IndexBitSet))
            return false;
    
        IndexBitSet o = (IndexBitSet) other;

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
}
