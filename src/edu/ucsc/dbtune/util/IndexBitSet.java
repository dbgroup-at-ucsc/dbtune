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
     * Removes all the indexes from the configuration.
     */
    public void clear()
    {
        bitSet.clear();
    }

    /**
     * Removes the given index from the configuration.
     */
    public void remove(int id)
    {
        bitSet.clear(id);
    }
    
    /**
     * Adds an index to the configuration. If the index is already contained it's not added.
     *
     * @param id
     *     new index to add
     */
    public void set(int id)
    {
        bitSet.set(id);
    }
    
    public int size()
    {
        return bitSet.length();
    }
    
    public boolean get(int id)
    {
        return bitSet.get(id);
    }
    
    public void clear(int id)
    {
        bitSet.clear(id);
    }
    
    public void andNot(IndexBitSet other)
    {
        bitSet.andNot(other.bitSet);
    }
    
    public void and(IndexBitSet other)
    {
        bitSet.and(other.bitSet);
    }
    
    /**
     * Adds an index to the configuration. If the index is already contained it's not added.
     *
     * @param from
     *     new index to add
     */
    public void or(IndexBitSet other)
    {
        bitSet.or(other.bitSet);
    }
    
    /**
     * @param other
     *      configuration added to this one
     */
    public final void set(IndexBitSet other)
    {
        bitSet.clear();
        bitSet.or(other.bitSet);
    }
    
    /**
     * @param other
     *      other configuration whose elements are compared against this one.
     * @return
     *      {@code true} if this configuration is a subset of other. {@code false} otherwise.
     */
    public final boolean subsetOf(IndexBitSet other)
    {
        synchronized (t) {
            t.clear();
            t.or(bitSet);
            t.and(other.bitSet);
            return t.equals(bitSet);
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
