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
import static edu.ucsc.dbtune.workload.SQLCategory.SELECT;
import static edu.ucsc.dbtune.bip.CandidateGeneratorFunctionalTest.readCandidateIndexes;

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
        // 1. test divergent
        testDivergent();
        
        // 2. Uniform design
        testUniform();
    }
    
    protected static void testDivergent() throws Exception
    {
        // 1. Read common parameter
        getEnvironmentParameters();
        candidates = readCandidateIndexes();
        
        // 2. set parameter for DivBIP()
        setParameters();
        
        if (!(io instanceof InumOptimizer))
            return;
        
        // split workload 
        splitSelectUpdateInWorkload();
                
        // 3. Call divergent design
        for (double B1 : listBudgets) {
            
            B = B1;            
            System.out.println(" Space:  " + B + "============\n");
            
            for (int i = 0; i < listNumberReplicas.size(); i++) {           
                
                nReplicas = listNumberReplicas.get(i);
                loadfactor = (int) Math.ceil( (double) nReplicas / 2);
                    
                System.out.println("----------------------------------------");
                System.out.println(" DIV-BIP, # replicas = " + nReplicas
                                    + ", load factor = " + loadfactor
                                    + ", space = " + B);

                if (isGetAverage) 
                    getAverageImbalanceFactor();
                else {
                    
                    testDiv();
                
                    // DB2 Cost
                    if (isDB2Cost)
                        compareWithDB2Cost();
                }
                
                System.out.println("----------------------------------------");
            }
        }
    }
    
    
    /**
     * Run the BIP with the parameters set by other functions
     * @throws Exception
     */
    public static void testDiv() throws Exception
    {
        div = new DivBIP();
        
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
            
            updateCost += div.getTotalBaseTableUpdateCost();
            // add the update-base-table-constant costs
            totalCostBIP = div.getObjValue() + div.getTotalBaseTableUpdateCost();
            
            System.out.println("CPLEX result: " 
                    + " TOTAL cost: " + totalCostBIP + "\n"
                    + " QUERY cost:  " + queryCost   + "\n"
                    + " UPDATE cost: " + updateCost  + "\n"
                    + " ----- Update cost details: "  + "\n"
                    + "          + query shell & update indexes: " 
                                        + div.getUpdateCostFromCplex() + "\n"
                    + "          + update base table:             "
                                        + div.getTotalBaseTableUpdateCost() + "\n"
                    + " ----- CPLEX info: \n"
                    + "          + obj value: " + div.getObjValue() + "\n"
                    + "          + gap from the optimal: " + div.getObjectiveGap() + "\n");
            
            if (isShowRecommendation)
                System.out.println("----Output: " + output);
           
                        
            System.out.println(" TOTAL COST(INUM): " + totalCostBIP);
            
            // show imbalance query & replica
            System.out.println(" IMBALANCE REPLICA: " + div.getMaxImbalanceReplica());
            System.out.println(" IMBALANCE QUERY: " + div.getMaxImbalanceQuery());   
            System.out.println(" NODE FAILURE: " + div.getMaxNodeFailure());
            
        } else 
            System.out.println(" NO SOLUTION ");
        
    }
    
        
    
    /**
     * Call UNIF
     */
    private static void testUniform() throws Exception
    {
        nReplicas = 1;
        loadfactor = 1;
        
        System.out.println("----------------------------------------");
        System.out.println(" DIV-UNIF-BIP, # replicas = " + nReplicas
                            + ", load factor = " + loadfactor
                            + ", space = " + B);
        
        testDiv();
        
        // get the query cost & update cost
        double queryCost;
        double updateCost;
        double totalCostUniform;
        
        // update index cost & query shell in update statement 
        updateCost = div.getUpdateCostFromCplex();
        // query-only cost
        queryCost  = div.getObjValue() - updateCost;
        // add the constant update-base table cost
        updateCost += div.getTotalBaseTableUpdateCost();
        
        // UNIF for the case with more than one replica
        for (int i = 0; i < listNumberReplicas.size(); i++){
            
            System.out.println("----------------------------------------");
            nReplicas = listNumberReplicas.get(i);
            
            totalCostUniform = queryCost + updateCost * nReplicas;            
            System.out.println(" DIV-UNIF, # replicas: " + nReplicas + "\n"
                                + " TOTAL COST: " + totalCostUniform + "\n"
                                + " QUERY cost: " + queryCost + "\n"
                                + " UPDATE cost: " + (updateCost * nReplicas) + "\n"
                                + " ---- Detailed UPDATE cost (ONE Replica): \n" 
                                + "         - query shell & update indexes: " +
                                             div.getUpdateCostFromCplex() + "\n"
                                + "         - base table                  : " 
                                +         div.getTotalBaseTableUpdateCost() + "\n");
            
            System.out.println("----------------------------------------");
        }
        
        if (!isDB2Cost)
            return;
        
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
        
        // UNIF for the case with more than one replica
        for (int i = 0; i < listNumberReplicas.size(); i++) { 
            
            nReplicas = listNumberReplicas.get(i);
            System.out.println(" DB2 COST----------------------------------------");
            totalCostUniform = queryCost + updateCost * nReplicas;            
            System.out.println(" DIV-UNIF, # replicas: " + nReplicas + "\n"
                                + " TOTAL COST: " + totalCostUniform + "\n"
                                + " UPDATE COST: " + updateCost + "\n"
                                + " QUERY cost: " + queryCost + "\n"
                                );
            
            System.out.println("----------------------------------------");
        }
        
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
        for (int r = 0; r < nReplicas; r++)
            updateCostDB2 += computeWorkloadCostDB2(wlUpdates, divConf.
                                    indexesAtReplica(r));
        
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
            testDiv();
            avgImbalanceQuery += div.getMaxImbalanceQuery();
            avgImbalanceReplica += div.getMaxImbalanceReplica();
            avgNodeFailure += div.getMaxNodeFailure();
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
