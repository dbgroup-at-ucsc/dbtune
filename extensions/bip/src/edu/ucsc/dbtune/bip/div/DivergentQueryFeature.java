package edu.ucsc.dbtune.bip.div;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.util.HashCodeUtil;

/**
 * Implement the method to compute similarity between two queries
 *  
 * @author Quoc Trung Tran
 *
 */
public class DivergentQueryFeature 
{   
    private Map<IndexAtReplica, Integer> presence;
    private List<IndexAtReplica> keys;
        
    /**
     * Construct a map assuming that no index is used
     * (for the moment)
     * 
     * @param conf
     *      The divergent design
     */
    public DivergentQueryFeature(DivConfiguration conf)
    {
        presence = new HashMap<IndexAtReplica, Integer>();
        keys = new ArrayList<IndexAtReplica>();
        IndexAtReplica key;
            
        for (int r = 0; r < conf.getNumberReplicas(); r++)
            for (Index index : conf.indexesAtReplica(r)) {
                key = new IndexAtReplica(index, r);
                presence.put(key, 0);
                keys.add(key);
            }   
    }

            
    /**
     * The optimal plan used the given index at the given replica
     * 
     * @param index
     *      The index
     * @param r
     *      The replica ID
     */
    public void addUsedIndexAtReplica (Index index, int r)
    {
        presence.put(new IndexAtReplica(index, r), 1);
    }
        
    /**
     * Retrieve the value of the corresponding key
     * 
     * @param key
     *      The key, which repsents the index at each replica
     *      
     * @return
     *      The corresponding value
     */
    public int getValue(IndexAtReplica key)
    {
        return presence.get(key);
    }
        
    /**
     * Compute the consine similarity = A x B / |A| * |B|
     * 
     * @param opponent
     *      The opponent
     * @return
     *      The similarity between two vectors
     */
    public double cosineSimilarity(DivergentQueryFeature opponent)
    {
        double numerator = 0.0;
        double denominator = 0.0;
            
        // 1. numerator = \sum_{keys} a_i * b_i
        for (IndexAtReplica key: keys)
            numerator += presence.get(key) 
                            * opponent.presence.get(key);
        // 2. denoninator = \sqrtroot(\sum_{a_i^2})
        //                * \sqrtroot(\sum_{b_i^2})
        double absoluteFirst = 0.0, absoluteSecond = 0.0;
        for (IndexAtReplica key: keys) {
            absoluteFirst += Math.pow(presence.get(key), 2);
            absoluteSecond += Math.pow(opponent.presence.get(key), 2);
        }
        denominator = Math.sqrt(absoluteFirst) 
                        * Math.sqrt(absoluteSecond);
        
        if (denominator == 0) {
            // both does not use any indexes
            if (absoluteFirst == 0 && absoluteSecond == 0)
                return 1.0;
            else 
                // one use indexes, the other does not
                return -1.0;
        }
        
        return numerator / denominator;
    }

    
    /**
     * Encode an index at a replica in a divergent design
     * 
     * @author Quoc Trung Tran
     *
     */
    public static class IndexAtReplica
    {
        private Index index;
        private int r;
        private int fHashCode;
        
        public IndexAtReplica(Index index, int r)
        {
            this.index = index;
            this.r = r;
        }
        
        @Override
        public boolean equals(Object obj) 
        {
            if (!(obj instanceof IndexAtReplica))
                return false;

            IndexAtReplica var = (IndexAtReplica) obj;
            
            if ( (this.index.getId() != var.index.getId()) ||
                 (this.r != var.r)) 
                return false;
            
            return true;
        }

        @Override
        public int hashCode() 
        {
            if (fHashCode == 0) {
                int result = HashCodeUtil.SEED;
                result = HashCodeUtil.hash(result, this.index.hashCode());
                result = HashCodeUtil.hash(result, this.r);
                fHashCode = result;
            }
            
            return fHashCode;
        }
    }
}
