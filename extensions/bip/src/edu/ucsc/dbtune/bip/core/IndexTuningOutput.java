package edu.ucsc.dbtune.bip.core;

import java.util.Set;

import edu.ucsc.dbtune.metadata.Index;

/**
 * The abstract class that stores the result of an index tuning related problem
 * This class will be implemented depending on the specific problem.
 * 
 * @author Quoc Trung Tran
 *
 */
public class IndexTuningOutput 
{
    private Set<Index> recommended;
    
    /**
     * Set the recommended indexes
     * 
     * @param s
     *      A set of indexes
     */
    public void setIndexes(Set<Index> s)
    {
        recommended = s;
    }
    
    /**
     * Retrieve the set of recommended indexes.
     * 
     * @return
     *      the recommended indexes 
     */
    public Set<Index> getRecommendation()
    {
        return recommended;
    }
}
