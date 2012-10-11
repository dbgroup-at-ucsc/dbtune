package edu.ucsc.dbtune.bip.util;

import java.util.Set;

import edu.ucsc.dbtune.util.HashCodeUtil;

/**
 * This class caches the computation of INUM for a given
 * SQL query and an index set.
 * 
 * This class is used for the experiments on the online feature
 * of DIVBIP that avoid excessive calls to INUM (and thus to DB2Advisor)
 * 
 * @author Quoc Trung Tran
 *
 */
public class CachedInumQueryCost 
{
    protected String sql;
    protected Set<Integer> indexIDs;
    private int fHashCode;
    
    /**
     * Construct this object
     * 
     * @param sql
     *      The SQL query
     * @param indexIDs
     *      An index set, given by the IDs of indexes in this set
     */
    public CachedInumQueryCost(String sql, Set<Integer> indexIDs)
    {
        this.sql = sql;
        this.indexIDs = indexIDs;
        this.fHashCode = 0;
    }
    
    @Override
    public boolean equals(Object obj) 
    {
        if (!(obj instanceof CachedInumQueryCost))
            return false;

        CachedInumQueryCost var = (CachedInumQueryCost) obj;
        
        if ( !(this.sql.equals(var.sql))   
             || (this.indexIDs.size() != var.indexIDs.size())
             )
            return false;
        
        // Check if the two has the same set of indexes
        Set<Integer> minus = this.indexIDs;
        minus.removeAll(var.indexIDs);
        
        if (minus.size() > 0)
            return false;
        
        return true;
    }

    @Override
    public int hashCode() 
    {
        if (fHashCode == 0) {
            int result = HashCodeUtil.SEED;
            result = HashCodeUtil.hash(result, this.sql.hashCode());
            for (Integer i : indexIDs)
                result = HashCodeUtil.hash(result, i);
            
            fHashCode = result;
        }
        
        return fHashCode;
    }
    
}
