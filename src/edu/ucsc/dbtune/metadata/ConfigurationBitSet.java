package edu.ucsc.dbtune.metadata;

import edu.ucsc.dbtune.util.IndexBitSet;

/**
 * A configuration that uses a bitset to efficiently operate on its contents.
 *
 * @author Ivo Jimenez
 */
public class ConfigurationBitSet extends Configuration
{
    protected IndexBitSet bitSet;

    public ConfigurationBitSet(Configuration configuration)
    {
    	super("");
    	for (Index index:configuration) {
    		add(index);
    	}
    }
    
    /**
     * Constructs a configuration.
     *
     * @param other
     *     other configuration whose indexes are referred by {@code bitSet}
     * @param bitSet
     *     the bitset that represents which indexes are turned on.
     */
    public ConfigurationBitSet(Configuration other, IndexBitSet bitSet)
    {
        super(other.toList());
        this.bitSet = bitSet;
    }

    /**
     * Returns the bitSet
     *
     * @return
     *     the bitset
     */
    public IndexBitSet getBitSet()
    {
        return bitSet;
    }
    
    /**
     * Adds an index to the configuration. If the index is already contained, the index is not added 
     * again.
     *
     * @param index
     *     new index to add
     */
    @Override
    public void add(Index index)
    {
        if (!this.contains(index))
        {
            _indexes.add(index);
            bitSet.set(_indexes.indexOf(index));
        }
    }

    /**
     * checks if a given index is contained in the configuration.
     *
     * @param index
     *     index that is looked for as part of this configuration
     */
    @Override
    public boolean contains(Index index)
    {
        return bitSet.get(_indexes.indexOf(index));
    }
}
