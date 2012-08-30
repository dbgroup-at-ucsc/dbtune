package edu.ucsc.dbtune.bip;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.junit.Test;


import edu.ucsc.dbtune.bip.core.IndexTuningOutput;
import edu.ucsc.dbtune.bip.div.DivBIP;
import edu.ucsc.dbtune.bip.div.DivConfiguration;
import edu.ucsc.dbtune.bip.util.LogListener;

import edu.ucsc.dbtune.optimizer.InumOptimizer;
import edu.ucsc.dbtune.optimizer.Optimizer;
import edu.ucsc.dbtune.workload.SQLStatement;
import edu.ucsc.dbtune.workload.Workload;

import static edu.ucsc.dbtune.bip.CandidateGeneratorFunctionalTest.readCandidateIndexes;
import static edu.ucsc.dbtune.workload.SQLCategory.SELECT;
import edu.ucsc.dbtune.metadata.Index;


/**
 * Test the functionality of Divergent Design using BIP.
 * 
 * @author Quoc Trung Tran
 *
 *
 */
public class DivBIPFunctionalTest extends DivTestSetting
{   
    private static Workload wlQueries;
    private static Workload wlUpdates;
    
    
    @Test
    public void main() throws Exception
    {
        // 1. Read common parameter
        getEnvironmentParameters();
        
        // 2. set parameter for DivBIP()
        setParameters();
        
        if (!(io instanceof InumOptimizer))
            return;
        
        candidates = readCandidateIndexes();        
        for (Index index : candidates)
            System.out.println(" Index " + index
                                + " id: " + index.getId()
                                + " size: " + (double) index.getBytes() / Math.pow(10, 6) + " (MB)"
                                + " creation cost: "  + index.getCreationCost());
        
                
        // 1. test divergent
        for (double B1 : listBudgets) 
            for (int n : listNumberReplicas)
                testDiv(n, B1);
        
        // 2. Uniform design
        // 3. Call divergent design
        for (double B1 : listBudgets) 
            for (int n : listNumberReplicas)
                testUniform(n, B1);
    }
    
    
    
    /**
     * Run the BIP with the parameters set by other functions
     * @throws Exception
     */
    public static double testDiv(int _n, double _B) throws Exception
    {
        div = new DivBIP();
        // Derive corresponding parameters
        nReplicas = _n;
        B = _B;
        loadfactor = (int) Math.ceil( (double) nReplicas / 2);
        
        Optimizer io = db.getOptimizer();
         
        LogListener logger = LogListener.getInstance();
        div.setCandidateIndexes(candidates);
        div.setWorkload(workload); 
        div.setOptimizer((InumOptimizer) io);
        div.setNumberReplicas(nReplicas);
        div.setLoadBalanceFactor(loadfactor);
        div.setSpaceBudget(B);
        div.setLogListenter(logger);
        
        IndexTuningOutput output = div.solve();
        System.out.println(logger.toString());
        
        if (isExportToFile)
            div.exportCplexToFile(en.getWorkloadsFoldername() + "/test.lp");
        
        double totalCostBIP;
        double updateCost;
        double queryCost;
        
        if (output != null) {
            
            divConf = (DivConfiguration) output;           
            updateCost = div.getUpdateCostFromCplex();
            queryCost = div.getObjValue() - updateCost;
            
            // NOT sum-up base table access cost for now
            //updateCost += div.getTotalBaseTableUpdateCost();
            // add the update-base-table-constant costs
            totalCostBIP = div.getObjValue(); // + div.getTotalBaseTableUpdateCost();
            
            System.out.println("CPLEX result: number of replicas: " + nReplicas + "\n" 
                    + " TOTAL cost:  " + totalCostBIP + "\n"
                    + " QUERY cost:  " + queryCost   + "\n"
                    + " UPDATE cost: " + updateCost  + "\n"
                    + " ----- Update cost details: "  + "\n"
                    + "          + query shell & update indexes: " 
                                        + div.getUpdateCostFromCplex() + "\n"
                    //+ "          + update base table:             "
                    //                    + div.getTotalBaseTableUpdateCost() + "\n"
                    + " ----- CPLEX info: \n"
                    + "          + obj value: " + div.getObjValue() + "\n"
                    + "          + gap from the optimal: " + div.getObjectiveGap() + "\n"
                    + " NUMBER of distinct indexes: " + divConf.countDistinctIndexes()+ "\n"
                    + " NUMBER OF queries:          " + 
                            div.computeNumberQueriesSpecializeForReplica()
                    //+ " Configuration: " + divConf
                    );
            
            //if (isShowRecommendation)
              //  System.out.println("----Output: " + output);
                                   
            System.out.println(" TOTAL COST(INUM): " + totalCostBIP);
            
            // show imbalance query & replica
            System.out.println(" IMBALANCE REPLICA: " + div.getNodeImbalance());
            System.out.println(" IMBALANCE QUERY: " + div.getQueryImbalance());   
            System.out.println(" NODE FAILURE: " + div.getFailureImbalance());
            
            return totalCostBIP;
            
        } else {
            System.out.println(" NO SOLUTION ");
            return -1;
        }
    }
    
    /**
     * Call UNIF
     */
    public static double testUniform (int n, double B1) throws Exception
    {   
        B = B1;
        nReplicas = 1;
        loadfactor = 1;
        
        System.out.println("----------------------------------------");
        System.out.println(" DIV-UNIF-BIP, # replicas = " + nReplicas
                            + ", load factor = " + loadfactor
                            + ", space = " + B);
        
        testDiv(1, B);
        
        // get the query cost & update cost
        double queryCost;
        double updateCost;
        double totalCostUniform;
        
        // update index cost & query shell in update statement 
        updateCost = div.getUpdateCostFromCplex();
        // query-only cost
        queryCost  = div.getObjValue() - updateCost;
        // NOT consider adding the constant update-base table cost
        // for the time-being
        // updateCost += div.getTotalBaseTableUpdateCost();
         
        // UNIF for the case with more than one replica
        System.out.println("----------------------------------------");
        nReplicas = n;
        totalCostUniform = queryCost + updateCost * nReplicas;            
        System.out.println(" DIV-UNIF, # replicas: " + nReplicas + "\n"
                + " TOTAL COST: " + totalCostUniform + "\n"
                + " QUERY cost: " + queryCost + "\n"
                + " UPDATE cost: " + (updateCost * nReplicas) + "\n"
                + " ---- Detailed UPDATE cost (ONE Replica): \n" 
                + "         - query shell & update indexes: " +
                div.getUpdateCostFromCplex() + "\n"
                //+ "         - base table                  : " 
                //+         div.getTotalBaseTableUpdateCost() + "\n"
        );
            
        System.out.println("----------------------------------------");
        
        if (!isDB2Cost)
            return totalCostUniform;
        
        // call DB2 to compute cost
        List<SQLStatement> querySQLs = new ArrayList<SQLStatement>();
        List<SQLStatement> updateSQLs = new ArrayList<SQLStatement>();
        
        for (SQLStatement sql : workload)
            if (sql.getSQLCategory().isSame(SELECT))
                querySQLs.add(sql);
            else 
                updateSQLs.add(sql);
        
        Workload wlQueries = new Workload(querySQLs);
        Workload wlUpdates = new Workload(updateSQLs);
        
        // compute query cost & update cost
        queryCost = computeWorkloadCostDB2(wlQueries, divConf.indexesAtReplica(0));
        updateCost = computeWorkloadCostDB2(wlUpdates, divConf.indexesAtReplica(0));
        
        nReplicas = n;
        System.out.println(" DB2 COST----------------------------------------");
        totalCostUniform = queryCost + updateCost * nReplicas;            
        System.out.println(" DIV-UNIF, # replicas: " + nReplicas + "\n"
                + " TOTAL COST: " + totalCostUniform + "\n"
                + " UPDATE COST: " + updateCost + "\n"
                + " QUERY cost: " + queryCost + "\n"
        );
        
        System.out.println("----------------------------------------");
        return totalCostUniform;
    }
    
    /**
     * This function compares the results of CPLEX with the cost computed by DB2
     * 
     */
    protected static void compareWithDB2Cost() throws Exception
    {   
        double queryCostDB2;
        double updateCostDB2;
        
        queryCostDB2 = 0.0;
        updateCostDB2 = 0.0;
        
        System.out.println("Configuration to COMPUTE DB2: "
                                + divConf.briefDescription());
        splitSelectUpdateInWorkload();
        
        for (int r = 0; r < nReplicas; r++)
            updateCostDB2 += computeWorkloadCostDB2(wlUpdates, divConf.indexesAtReplica(r));
        
        // update cost (tricky)
        for (SQLStatement sql : wlQueries) {
            List<Double> costs = computeQueryCostsDB2(sql, divConf);
            Collections.sort(costs);
            
            for (int k = 0; k < loadfactor; k++)
                queryCostDB2 += costs.get(k) / loadfactor;
        }
        
        System.out.println("------------ DB2 -------------\n"
                           + " TOTAL cost: " + (queryCostDB2 + updateCostDB2) + "\n"
                           + " QUERY cost: " + queryCostDB2 + "\n"
                           + " UPDATE cost: " + updateCostDB2 + "\n"
                           );

    }
    
    /**
     * Classify workload according to query or update statements
     */
    private static void splitSelectUpdateInWorkload()
    {
        // call DB2 to compute cost
        List<SQLStatement> querySQLs = new ArrayList<SQLStatement>();
        List<SQLStatement> updateSQLs = new ArrayList<SQLStatement>();
        
        for (SQLStatement sql : workload)
            if (sql.getSQLCategory().isSame(SELECT))
                querySQLs.add(sql);
            else 
                updateSQLs.add(sql);
        
        wlQueries = new Workload(querySQLs);
        wlUpdates = new Workload(updateSQLs);
        
    }
    
    /**
     * Run the instance of the problem five times and get the average 
     * of imbalance query, replica, and node failure
     * 
     * @throws Exception
     */
    public static void getAverageImbalanceFactor() throws Exception
    {
        // run four more times and get the average of imbalance query & replica
        int numRuns;
        double avgImbalanceQuery;
        double avgImbalanceReplica;
        double avgNodeFailure;
        
        avgImbalanceQuery = 0.0;
        avgImbalanceReplica = 0.0;
        avgNodeFailure = 0.0;
        
        numRuns = 5;
        
        for (int iterImbalance = 0; iterImbalance < numRuns; iterImbalance++) {
            testDiv(nReplicas, B);
            avgImbalanceQuery += div.getQueryImbalance();
            avgImbalanceReplica += div.getNodeImbalance();
            avgNodeFailure += div.getFailureImbalance();
        }
        
        
        System.out.println("================== Number of replicas: " +  nReplicas
                            + " AVG Imbalance QUERY: "
                            + (double) avgImbalanceQuery / numRuns);
        System.out.println("================== Number of replicas: " +  nReplicas
                            + " AVG Imbalance REPLICA: "
                            + (double) avgImbalanceReplica / numRuns);
        
        System.out.println("================== Number of replicas: " +  nReplicas
                + " AVG NODE failure: "
                + (double) avgNodeFailure / numRuns);
    }

}
