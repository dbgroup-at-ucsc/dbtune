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
    protected List<Set<Index>> recommend(Workload workload, int nReplicas, int loadfactor, double B)
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
        
        // return recommended indexes at every replica
        return indexesAtReplica;
    }
     
    /**
     * The main method that implements DivDesign algorithm. 
     */
    private void process() throws Exception
    {
        // 1. initialize partitioning statements into replicas
        // using random method.
        initializePartition();
        
        // 2. repeat the while loop while the improvement is high 
        // or exceeds the number of iterations
        double relativeGap = 0.0;
        
        double previousCost = 0.0;
        System.out.println("BEFORE THE LOOP ");
        
        iter = 0;
        while (iter < maxIters) {
            
            // recommend index at each replica
            for (int i = 0; i < n; i++) {
                long start = System.currentTimeMillis();
                indexesAtReplica.set(i, getRecommendation(i));
                System.out.println("L99, time to get recommendation: "  + i + " : "
                                    + (System.currentTimeMillis() - start));
            }
            
            
            // calculate the total cost and repartitions statements 
            calculateTotalCostAndRepartition();
            
            // check if we trap in the local optimal
            relativeGap = Math.abs(totalCost - previousCost) /  Math.max(totalCost, previousCost);
            System.out.println("L105, iteraction #" + iter
                                + " relative gap: " + relativeGap);
            
            // we have only one replica, do not need to repartition
            if (relativeGap < epsilon || n == 1)
                break;
            
            previousCost = totalCost;
            iter++;
        }
    }
    
    public int getNumberOfIterations()
    {
        return iter;
    }
    
    
    
    /**
     * Create an initial partitioning of the input workload
     * 
     *      
     */
    private void initializePartition()
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
        Random random = new Random(19580427);
        Set<Integer> partition;
        
        for (int j = 0; j < workload.size(); j++) {
            
            sql = workload.get(j);
            
            partition = new HashSet<Integer>();
            
            // update statement ==> send to all partitions
            if (sql.getSQLCategory().isSame(NOT_SELECT))
                for (int i = 0; i < n; i++)
                    partition.add(i);
            else 
            // randomly place the statement into m replicas   
                while (partition.size() < m)
                    partition.add(random.nextInt(n));
            
            for (Integer i : partition)
                stmtIDInPartitions.get(i).add(j);
            
        }        
    }
    
    /**
     * Caculate the total cost and repartition statements
     */
    private void calculateTotalCostAndRepartition() throws Exception
    {
        List<QueryCostAtPartition> costs; 
        List<Integer>  partitionIDs;
        SQLStatement sql;
        
        // prepare to repartition
        for (int i = 0; i < n; i++)
            stmtIDInPartitions.get(i).clear();
        
        // calculate the total cost
        totalCost = 0.0;
        for (int j = 0; j < workload.size(); j++) {
            
            sql   = workload.get(j);
            costs = statementCosts(sql);
            partitionIDs = new ArrayList<Integer>();
            
            // update statement
            if (sql.getSQLCategory().isSame(NOT_SELECT)) {
                
                totalCost += costs.get(0).getCost();
                for (int i = 0; i < n; i++)
                    partitionIDs.add(i);
                
            }
            else {
                // an query statement, get the top-m best cost
                Collections.sort(costs);
                
                for (int k = 0; k < m; k++) {
                    totalCost += costs.get(k).getCost() / m;
                    partitionIDs.add(costs.get(k).getPartitionID());
                }
                
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
    private Set<Index> getRecommendation(int partitionID) throws Exception
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
     * Retrieve the cost of the given statement w.r.t. the configuration at each replica
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
