package edu.ucsc.dbtune.bip.div;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import edu.ucsc.dbtune.bip.core.IndexTuningOutput;
import edu.ucsc.dbtune.metadata.Index;


public class DivConfiguration extends IndexTuningOutput
{
    private int              nReplicas;
    private int              loadfactor;
    private List<Set<Index>> indexReplicas;
    
    /**
     * Construct an object with a given number of replica
     * 
     * @param nReplicas
     *      The number of replicas to deploy the database
     */
    public DivConfiguration(int nReplicas, int m)
    {
        this.nReplicas  = nReplicas;
        this.loadfactor = m;
        indexReplicas   = new ArrayList<Set<Index>>();
        
        for (int r = 0; r < nReplicas; r++)
            indexReplicas.add(new HashSet<Index>());
        
    }
    
    /**
     * Count the different indexes that are recommended at different replicas
     * 
     * @return
     *      The number of distinct indexes
     */
    public int countDistinctIndexes()
    {
       Set<Index> indexes = new HashSet<Index>();
       
       for (int i = 0; i < nReplicas; i++)
           indexes.addAll(indexesAtReplica(i));
       
       return indexes.size();
    }
    
    /**
     * Copy constructor 
     * 
     * @param divConf
     *      The source configuration to be copied.
     *      
     */
    public DivConfiguration(DivConfiguration divConf) 
    {
        this.nReplicas = divConf.nReplicas;
        this.loadfactor = divConf.loadfactor;
        
        indexReplicas   = new ArrayList<Set<Index>>();
        
        for (int r = 0; r < nReplicas; r++) 
            indexReplicas.add(new HashSet<Index>(divConf.indexesAtReplica(r)));
        
    }
    
    /**
     * This acts similar to a copy constructor, except removing empty replicas
     * 
     * 
     * @param divConf
     *  todo
     */
    public void copyAndRemoveEmptyConfiguration(DivConfiguration divConf) 
    {
        nReplicas = 0;
        loadfactor = divConf.loadfactor;
        
        indexReplicas   = new ArrayList<Set<Index>>();
        
        for (int r = 0; r < divConf.nReplicas; r++) {
            if (divConf.indexesAtReplica(r).size() > 0) {
                indexReplicas.add(new HashSet<Index>(divConf.indexesAtReplica(r)));
                nReplicas++;
            }
        }        
    }

    /**
     * Retrieve the number of replicas in the system
     *  
     * @return
     *      The number of replicas
     */
    public int getNumberReplicas()
    {
        return nReplicas;
    }
    
    /**
     * Retrieve the load balancing factor (i.e., m)
     * 
     * @return
     *      The load factor.
     */
    public int getLoadFactor()
    {
        return loadfactor;
    }
    
    
    /**
     * Retrieve the set of indexes materialized at the given replica
     * 
     * @param r
     *      The replica ID
     *      
     * @return
     *      A set of indexes or throw exception if the replica ID is greater or equal to
     *      the total number of replicas.
     */
    public Set<Index> indexesAtReplica(int r)
    {
        if (r >= nReplicas)
            throw new RuntimeException("The replica ID exceeds the total number of replicas");
        
        return indexReplicas.get(r);
    }
    
    
    /**
     * Materialize an index at a particular replica
     * 
     * @param r
     *      The replica ID
     * @param index
     *      The index that is materialized
     */
    public void addIndexReplica(int r, Index index)
    {
        indexReplicas.get(r).add(index);
    }

    @Override
    public Set<Index> getRecommendation()
    {
        throw new RuntimeException("This method is not enabled in this subclass");
    }
    
    @Override
    public void setIndexes(Set<Index> s)
    {
        throw new RuntimeException("This method is not enabled in this subclass");
    }

    @Override
    public String toString() 
    {
        StringBuilder result = new StringBuilder();
        
        result.append("Divergent Configuration")
              .append("[Nreplicas=" + nReplicas)
              .append("\n");
        
                        
        for (int r = 0; r < nReplicas; r++) {
            
            result.append("Replica " + r + "-th: \n");
            
            for (Index index : indexReplicas.get(r))
                result.append(" CREATE INDEX "  + index + index.getId() + "\n");
            
            result.append("\n");
        }
        
        return result.toString();
    }
    
    /**
     * Return a string contains a short description (only index ID) of the recommendation.
     * 
     * @return
     *      The string description
     */
    public String briefDescription()
    {
        StringBuilder result = new StringBuilder();
    
        result.append("Divergent Configuration")
              .append("[Nreplicas=" + nReplicas)
              .append("\n");
                  
        for (int r = 0; r < nReplicas; r++) {
      
            result.append("[");
      
            for (Index index : indexReplicas.get(r))
                result.append(index.getId() + ",");
                       
            result.append("]\n");
        }
        
        return result.toString();
    }
}
