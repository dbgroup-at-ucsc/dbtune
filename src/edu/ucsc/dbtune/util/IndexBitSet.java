package edu.ucsc.dbtune.util;

import java.util.BitSet;

/**
 * The structure used to represent an index configuration. Semantically, every {@code IndexBitSet} 
 * is tied to a {@link java.util.Collection} (instantiated with {@link 
 * edu.ucsc.dbtune.metadata.Index}. Thus, the user of this class is responsible of maintaining this 
 * {@code List<DBIndex> <-> IndexBitSet} mapping.
 * <p>
 * Example:
 * <code>
 * List<DBIndex> conf = getCandidatePool();
 * IndexBitSet   bs   = new IndexBitSet();
 * 
 * for (I index : pool) {
 *     if (pool.get(index.internalId()).state == State.RECOMMENDED){
 *         bs.set(index.internalId());
 *     }
 * }
 * </code>
 * <p>
 * In the example, the relation between {@code pool} and {@code bs} is maintained through the {@code 
 * index.internalId()} mechanism. If pool is modified, then {@code bs} should be modified 
 * accordingly.
 * 
 * <p>
 * Alkis: This documentation is inconsistent with the usage of {@code IndexBitSet} in {@link ConfigurationBitSet}.  
 */
public class IndexBitSet extends BitSet
{
    private static final long serialVersionUID = 1L;
    
    private static final BitSet t = new BitSet();
    
    public IndexBitSet() {
        super();
    }

    @Override
    public IndexBitSet clone() {
        return (IndexBitSet) super.clone();
    }
    
    public final void set(IndexBitSet other) {
        clear();
        or(other);
    }
    
    // probably better in average case
    public final boolean subsetOf(IndexBitSet b) {
        synchronized (t) {
            t.clear();
            t.or(this);
            t.and(b);
            return (t.equals(this));
        }
    }
}
