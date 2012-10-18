package edu.ucsc.dbtune.bip.div;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import edu.ucsc.dbtune.bip.core.IndexTuningOutput;
import edu.ucsc.dbtune.metadata.Index;


/**
 * A divergent design, including the set of index configurations
 * at each replica together with a mapping function
 * 
 * @author Quoc Trung Tran
 *
 */
public class DivConfiguration extends IndexTuningOutput
        implements Serializable
{
    private static final long serialVersionUID = 1L;

    private int nReplicas;
    private int loadfactor;
    private List<Set<Index>> indexReplicas;
    
    // Map each query to the set of replicas
    // that this query is sent to
    private RoutingFunction mappingWithoutFailure;
    private List<RoutingFunction> mappingWithFailures;
    
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
        mappingWithoutFailure = new RoutingFunction();
        mappingWithFailures = new ArrayList<RoutingFunction>();
        
        for (int r = 0; r < nReplicas; r++) {
            indexReplicas.add(new HashSet<Index>());
            
            RoutingFunction rf = new RoutingFunction();
            mappingWithFailures.add(rf);
        }
        
    }
    
    /**
     * Retrieve the set of replicas where the given query 
     * can be routed to
     * 
     * @param q
     * @return
     */
    public Set<Integer> getRoutingReplica(int q)
    {
        return mappingWithoutFailure.getRoutingReplica(q);
    }
    
    /**
     * Retrieve the set of replicas where the given query 
     * can be routed to
     * 
     * @param q
     * @return
     */
    public Set<Integer> getRoutingReplicaUnderFailure(int q, int failR)
    {
        return mappingWithFailures.get(failR).getRoutingReplica(q);
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
        
        this.mappingWithoutFailure = divConf.mappingWithoutFailure;
        
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
        
        indexReplicas = new ArrayList<Set<Index>>();
        
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
    
    /**
     * Add the information of routing query {@code q}
     * to replica {@code r}
     * 
     * @param q
     *      The query to be routed
     * @param r
     *      The replica where the query is routed
     */
    public void routeQueryToReplica(int q, int r)
    {
        mappingWithoutFailure.routeQueryToReplica(q, r);
    }
    
    /**
     * Add the information of routing query {@code q}
     * to replica {@code r}
     * 
     * @param q
     *      The query to be routed
     * @param r
     *      The replica where the query is routed
     * @param failR
     *      Assume this replica fails
     *      
     */
    public void routeQueryToReplicaUnderFailure(int q, int r, int failR)
    {
        mappingWithFailures.get(failR).routeQueryToReplica(q, r);
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
    
    /**
     * Show the number of indexes per replica and 
     * the maximum creation cost
     */
    public String indexAtReplicaInfo()
    {
        StringBuilder sb = new StringBuilder();
        
        double costAtReplica;
        double totalCreationCost = 0.0;
        double maxCreationCost = -1; 
            
        sb.append("Number of indexes at each replica = [");
        for (int r = 0; r < nReplicas; r++) {
            sb.append(indexReplicas.get(r).size());
            sb.append(",");
            
            costAtReplica = 0.0;
            for (Index i : indexReplicas.get(r))
               costAtReplica += i.getCreationCost();
            
            if (costAtReplica > maxCreationCost)
                maxCreationCost = costAtReplica;
            
            totalCreationCost += costAtReplica;
        }
        sb.append("]\n");
        
        sb.append("TOTAL creation cost = " + totalCreationCost + "\n")
          .append("MAX creation cost = " + maxCreationCost + "\n");
        
        return sb.toString();
    }
    
    /**
     * Compute the transition cost from the given {@code initial} configuration
     * to this configuration.
     * 
     * @param intial
     *      The initial configuration
     *      
     * @return
     *      The transition cost from {@code initial} to this configuration
     */
    public double transitionCost(DivConfiguration initial, boolean isParallelMode)
    {
        double transCost;
        double tranCostReplica;
        // Heuristic solution
        // Order the replica in the decreasing order of their indexes
        // For each replica, find the most matching         
        Set<Integer> hasMatchedReplicas = new HashSet<Integer>();
        Set<Integer> hasMatchedInitial = new HashSet<Integer>();
        Set<Index> initialReplica;
        int maxIndexes, minNewIndexes;
        int rMax, rMin;
        
        transCost = 0.0;
        while (true) {
            
            maxIndexes = -1;
            rMax = -1;
            for (int r = 0; r < nReplicas; r++){
                if (hasMatchedReplicas.contains(r))
                    continue;
                
                if (indexReplicas.get(r).size() > maxIndexes){
                    rMax = r;
                    maxIndexes = indexReplicas.get(r).size();
                }
            }
            
            // we are done
            if (rMax == -1)
                break;
            
            hasMatchedReplicas.add(rMax);
            
            rMin = -1;
            minNewIndexes = 9999;
            for (int r = 0; r < initial.getNumberReplicas(); r++){
                if (hasMatchedInitial.contains(r))
                    continue;
                
                initialReplica = new HashSet<Index>(indexReplicas.get(rMax));
                initialReplica.removeAll(initial.indexesAtReplica(r));
                if (initialReplica.size() < minNewIndexes){
                    minNewIndexes = initialReplica.size();
                    rMin = r;
                }
            }
            
            hasMatchedInitial.add(rMin);
            initialReplica = new HashSet<Index>(indexReplicas.get(rMax));
            initialReplica.removeAll(initial.indexesAtReplica(rMin));
            
            tranCostReplica = 0.0;
            for (Index index : initialReplica)
                tranCostReplica += index.getCreationCost();
            
            if (!isParallelMode)
                transCost += tranCostReplica;
            else {
                // parallel mode
                // thus get the maximum transformation cost
                // at each replica
                if (tranCostReplica > transCost)
                    transCost = tranCostReplica;
            }
                
        }    
        
        return transCost;
    }
    
    static class RoutingFunction  implements Serializable
    {
        private static final long serialVersionUID = 1L;
        private Map<Integer, Set<Integer>> mapping;
        
        public RoutingFunction()
        {
            mapping = new HashMap<Integer, Set<Integer>>();
        }
        
        /**
         * Route the given query to the given replica
         * 
         * @param q
         *      the query ID
         * @param r
         *      the replica ID
         */
        public void routeQueryToReplica(int q, int r)
        {
            Set<Integer> replicas;
            if (!mapping.containsKey(q))
                replicas = new HashSet<Integer>();
            else 
                replicas = mapping.get(q);
            
            replicas.add(r);
            mapping.put(q, replicas);
        }
        
        /**
         * Retrieve the routing replicas for the given query
         * 
         * @param q
         *      The given query
         * @return
         *      List of routing replicas
         */
        public Set<Integer> getRoutingReplica(int q)
        {
            if (mapping.containsKey(q))
                return mapping.get(q);
            else
                throw new RuntimeException("ERROR, does not know" +
                       " how to route query " + q);
        }
    }
}
