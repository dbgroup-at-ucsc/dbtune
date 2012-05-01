package edu.ucsc.dbtune.divgdesign;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.workload.SQLStatement;
import edu.ucsc.dbtune.workload.Workload;

import static edu.ucsc.dbtune.workload.SQLCategory.NOT_SELECT;

public abstract class DivgDesign 
{   
    // input paramters
    protected int n;
    protected int m;
    protected double B;
    protected Workload workload;
    
    // constrol knobs
    protected int maxIters;
    protected int iter;
    protected double epsilon;
    
    // statement and indexes at each replica
    protected List<Set<Integer>> stmtIDInPartitions;
    protected List<Set<Index>> indexesAtReplica;
    protected double totalCost;
    protected double updateCost;
    protected double queryCost;
        
    /**
     * Retrieve the total cost of the recommended configuration.
     * 
     * @return
     *      The total cost
     */
    public double getTotalCost()
    {
        return totalCost;
    }
    
    /**
     * Retrieve the total query cost
     * 
     * @return
     *      The query cost
     */
    public double getQueryCost()
    {
        return queryCost;
    }
    
    /**
     * Retrieve the total update cost
     * 
     * @return
     *      The update cost
     */
    public double getUpdateCost()
    {
        return updateCost;
    }
    
    /**
     * Retrieve the number of iterations that have been performed
     * 
     * @return
     *      The number of iterations
     */
    public int getNumberOfIterations()
    {
        return iter;
    }
    
    /**
     * Retrieve the recommendation, including indexes at each replica
     * 
     * @return
     *      The recommendation by the algorithm. 
     */
    public List<Set<Index>> getRecommendation()
    {
        return indexesAtReplica;
    }
    /**
     * Recommend the divergent configuration. 
     *
     * @param workload
     *      sql statements
     * @param nReplicas
     *      number of replicas
     * @param loadfactor
     *      the load-balance factor
     * @param B
     *      space budget
     *                     
     * @throws SQLException
     *      if the given statements can't be processed
     */
    public void recommend(Workload workload, int nReplicas, int loadfactor, double B)
                         throws Exception
    {
        this.workload = workload;
        this.n = nReplicas;
        this.m = loadfactor;
        this.B = B;

        this.maxIters = 30;
        this.epsilon = 0.05;
        
        // process 
        process();
    }
     
    /**
     * The main method that implements DivDesign algorithm. 
     */
    protected void process() throws Exception
    {
        // 1. initialize partitioning statements into replicas
        // using random method.
        initializePartition();
        
        // 2. repeat the while loop while the improvement is high 
        // or exceeds the number of iterations
        double relativeGap = 0.0;
        double previousCost = 0.0;
        
        iter = 0;
        while (iter < maxIters) {
            
            // recommend index at each replica
            for (int i = 0; i < n; i++)
                indexesAtReplica.set(i, getRecommendation(i));
                        
            // calculate the total cost and repartitions statements 
            calculateTotalCostAndRepartition();
            
            // check if we trap in the local optimal
            relativeGap = Math.abs(totalCost - previousCost) /  Math.max(totalCost, previousCost);
            //System.out.println("L105, iteraction #" + iter
              //                  + " relative gap: " + relativeGap);
            
            // we have only one replica, do not need to repartition
            if (relativeGap < epsilon || n == 1)
                break;
            
            previousCost = totalCost;
            iter++;
        }
    }
    
    
    /**
     * Create an initial partitioning of the input workload
     * 
     *      
     */
    protected void initializePartition()
    {
        // partition
        stmtIDInPartitions = new ArrayList<Set<Integer>>();
        
        for (int i = 0; i < n; i++) 
            stmtIDInPartitions.add(new HashSet<Integer>());
        
        // indexes
        indexesAtReplica = new ArrayList<Set<Index>>();
        for (int i = 0; i < n; i++)
            indexesAtReplica.add(new HashSet<Index>());
            
        // randomly assign statements to partitions
        SQLStatement sql;
        //Random random = new Random(19580427);
        Random random = new Random();
        Set<Integer> replicaIDsForStatement;
        
        for (int j = 0; j < workload.size(); j++) {
            
            sql = workload.get(j);
            
            replicaIDsForStatement = new HashSet<Integer>();
            
            // update statement ==> send to all partitions
            if (sql.getSQLCategory().isSame(NOT_SELECT))
                for (int i = 0; i < n; i++)
                    replicaIDsForStatement.add(i);
            else 
            // randomly place the statement into m replicas   
                while (replicaIDsForStatement.size() < m)
                    replicaIDsForStatement.add(random.nextInt(n));
            
            for (Integer i : replicaIDsForStatement)
                stmtIDInPartitions.get(i).add(j);
            
        }        
    }
    
    /**
     * Calculate the total cost and repartition statements
     */
    protected void calculateTotalCostAndRepartition() throws Exception
    {
        List<QueryCostAtPartition> costs; 
        List<Integer>  partitionIDs;
        SQLStatement sql;
        
        // prepare to repartition
        for (int i = 0; i < n; i++)
            stmtIDInPartitions.get(i).clear();
        
        // calculate the total cost
        totalCost = 0.0;
        updateCost = 0.0;
        queryCost = 0.0;
        for (int j = 0; j < workload.size(); j++) {
            
            sql   = workload.get(j);
            costs = statementCosts(sql);
            partitionIDs = new ArrayList<Integer>();
            
            // update statement
            if (sql.getSQLCategory().isSame(NOT_SELECT)) {
                
                for (int i = 0; i < n; i++) {
                    partitionIDs.add(i);
                    totalCost += costs.get(i).getCost();
                    updateCost += costs.get(i).getCost();
                }
                
            }
            else {
                // an query statement, get the top-m best cost
                Collections.sort(costs);
                
                for (int k = 0; k < m; k++) {
                    totalCost += costs.get(k).getCost() / m;
                    queryCost += costs.get(k).getCost() / m;
                    partitionIDs.add(costs.get(k).getPartitionID());
                }
                
                // One trick by Jeff:
                // For one query q that has the same costs w.r.t. two replicas r1, r2
                // We should place q into the one that has the least totalCost so far
                // or randomly pick the replica 
                
            }
            
            // repartition the query
            for (Integer i : partitionIDs)
                stmtIDInPartitions.get(i).add(j);
            
        }
    }
    
    /**
     * Get the recommendation for the statements that are in the given partition
     * 
     * @param partitionID
     *      The partition ID
     * @return
     */
    protected Set<Index> getRecommendation(int partitionID) throws Exception
    {   
        List<SQLStatement> sqls = new ArrayList<SQLStatement>();
        
        for (Integer j : stmtIDInPartitions.get(partitionID))
            sqls.add(workload.get(j));
        
        return getRecommendation(sqls);
    }
    
    /**
     * Returns the configuration obtained by the Advisor (e.g., DB2 or CoPhy)
     *
     * @return
     *      a {@code Configuration} object containing the information related to
     *      the recommendation produced by the advisor.
     * @throws SQLException
     *      if the given statement can't be processed
     */
    protected abstract Set<Index> getRecommendation(List<SQLStatement> sqls) 
              throws Exception;
    
    /**
     * Retrieve the cost of the given statement w.r.t. the configuration at each replica.
     * (Also take into account the weight)
     *     
     * @param sql
     *      The SQL statement
     * @return
     *      a list of cost of the statement on every replica.
     */
    protected abstract List<QueryCostAtPartition> statementCosts(SQLStatement sql) 
              throws Exception;
    
    
    class QueryCostAtPartition implements Comparable<QueryCostAtPartition>
    {
        private int id;
        private double cost;
        
        public QueryCostAtPartition(int id, double cost)
        {
            this.id = id;
            this.cost = cost;
        }
        
        public int getPartitionID()
        {
            return id;
        }
        
        public double getCost()
        {
            return cost;
        }
        
        
        @Override
        public int compareTo(QueryCostAtPartition o) 
        {       
            double objCost = o.cost; 
            if (cost < objCost)
                return -1;
            else if (cost == objCost)
                return 0;
            else 
                return 1;
        }
    }
    
}
