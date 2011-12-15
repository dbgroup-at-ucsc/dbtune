package edu.ucsc.dbtune.util;

import java.util.BitSet;

/**
 * A configuration represents a set of physical structures over tables (and/or columns) that are 
 * used to improve the performance of DML statements in a database. A configuration is typically 
 * composed of a set of indexes, but can also contain materialized views (MV), partitions, 
 * denormalizations, etc.
 *
 * @author Karl Schnaitter
 * @author Ivo Jimenez
 */
public class IndexBitSet
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
    public void clear()
    {
        bitSet.clear();
    }
    
    /**
     * {@inheritDoc}
     */
    public void add(int id)
    {
        bitSet.set(id);
    }
    
    /**
     * {@inheritDoc}
     */
    public int size()
    {
        return bitSet.length();
    }
    
    /**
     * {@inheritDoc}
     */
    public boolean contains(int id)
    {
        return bitSet.get(id);
    }
    
    /**
     * {@inheritDoc}
     */
    public void remove(int id)
    {
        bitSet.clear(id);
    }
    
    /**
     * {@inheritDoc}
     */
    public void removeAll(IndexBitSet other)
    {
        bitSet.andNot(other.bitSet);
    }
    
    /**
     * {@inheritDoc}
     */
    public void retainAll(IndexBitSet other)
    {
        bitSet.and(other.bitSet);
    }
    
    /**
     * {@inheritDoc}
     */
    public final void addAll(IndexBitSet other)
    {
        bitSet.or(other.bitSet);
    }
    
    /**
     * {@inheritDoc}
     */
    public final boolean contains(IndexBitSet other)
    {
        synchronized (t) {
            t.clear();
            t.or(other.bitSet);
            t.and(bitSet);
            return t.equals(other.bitSet);
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
}
