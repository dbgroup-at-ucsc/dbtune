package edu.ucsc.dbtune.bip;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.junit.Test;


import edu.ucsc.dbtune.advisor.db2.DB2Advisor;
import edu.ucsc.dbtune.bip.core.IndexTuningOutput;
import edu.ucsc.dbtune.bip.core.QueryPlanDesc;
import edu.ucsc.dbtune.bip.div.DivBIP;
import edu.ucsc.dbtune.bip.div.DivConfiguration;
import edu.ucsc.dbtune.bip.util.LogListener;

import edu.ucsc.dbtune.optimizer.InumOptimizer;
import edu.ucsc.dbtune.optimizer.Optimizer;
import edu.ucsc.dbtune.util.Rt;
import edu.ucsc.dbtune.workload.SQLStatement;
import edu.ucsc.dbtune.workload.Workload;

import static edu.ucsc.dbtune.bip.CandidateGeneratorFunctionalTest.readCandidateIndexes;
import static edu.ucsc.dbtune.workload.SQLCategory.SELECT;


/**
 * Test the functionality of DIV-BIP
 * 
 * @author Quoc Trung Tran
 *
 *
 */
public class DivBIPFunctionalTest extends DivTestSetting
{   
    @Test
    public void main() throws Exception
    {
        // 1. Read common parameter
        getEnvironmentParameters();
        
        // 2. set parameter for DivBIP()
        setParameters();
        
        if (!(io instanceof InumOptimizer))
            return;
        
        db2Advis = new DB2Advisor(db);
        candidates = readCandidateIndexes(db2Advis);
                
        // 3. Test the benefit of indexes
        //compareDB2InumQueryCosts(workload, candidates);
        
        
        // 3. test divergent
        for (double B1 : listBudgets) 
            for (int n : listNumberReplicas)
                testDiv(n, B1, false);
              
    }
    
    
    /**
     * Run the BIP with the parameters set by other functions
     * @throws Exception
     */
    public static double testDiv(int _n, double _B, boolean isOnTheFly) throws Exception
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
        div.setCommunicatingInumOnTheFly(isOnTheFly);
        
        IndexTuningOutput output = div.solve();
        Rt.p(logger.toString());
        
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
            
            Rt.p("CPLEX result: number of replicas: " + nReplicas + "\n" 
                    + " TOTAL cost:  " + totalCostBIP + "\n"
                    + " QUERY cost:  " + queryCost   + "\n"
                    + " UPDATE cost: " + updateCost  + "\n"
                    + " ----- Update cost details: "  + "\n"
                    + "          + query shell & update indexes: " 
                                        + div.getUpdateCostFromCplex() + "\n"
                    + " ----- CPLEX info: \n"
                    + "          + obj value: " + div.getObjValue() + "\n"
                    + "          + gap from the optimal: " + div.getObjectiveGap() + "\n"
                    + " NUMBER of distinct indexes: " + divConf.countDistinctIndexes()+ "\n"
                    + " NUMBER OF queries:          " + 
                            div.computeNumberQueriesSpecializeForReplica()
                    );
            
            Rt.p(" TOTAL COST(INUM): " + totalCostBIP);
            Rt.p(" NODE IMBALANCE: " + div.getNodeImbalance());
            //Rt.p(" PREDICTABILITY IMBALANCE: " + div.getQueryImbalance());   
            //Rt.p(" FAILURE IMBALANCE: " + div.getFailureImbalance());
            // check for node balance constraint
            /*
            div.setUpperReplicaCost(totalCostBIP * 1.1 / nReplicas);
            output = div.solve();
            Rt.p(logger.toString());
            if (output != null)
                Rt.p(" COST OF NODE-BALANCE CONSTRAINT " + div.getObjValue()
                        + " NODE IMBALANCE: " + div.getNodeImbalance()
                        + " NEW RATIO: " + (div.getObjValue() / totalCostBIP));
            */
            return totalCostBIP;
            
        } else {
            Rt.p(" NO SOLUTION ");
            return -1;
        }
    }
    
    /**
     * Run the BIP with the parameters set by other functions
     * @throws Exception
     */
    public static double testDiv(int _n, double _B, List<QueryPlanDesc> descs) throws Exception
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
        div.setQueryPlanDesc(descs);
        
        IndexTuningOutput output = div.solve();
        Rt.p(logger.toString());
        
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
            
            Rt.p("CPLEX result: number of replicas: " + nReplicas + "\n" 
                    + " TOTAL cost:  " + totalCostBIP + "\n"
                    + " QUERY cost:  " + queryCost   + "\n"
                    + " UPDATE cost: " + updateCost  + "\n"
                    + " ----- Update cost details: "  + "\n"
                    + "          + query shell & update indexes: " 
                                        + div.getUpdateCostFromCplex() + "\n"
                    + " ----- CPLEX info: \n"
                    + "          + obj value: " + div.getObjValue() + "\n"
                    + "          + gap from the optimal: " + div.getObjectiveGap() + "\n"
                    + " NUMBER of distinct indexes: " + divConf.countDistinctIndexes()+ "\n"
                    + " NUMBER OF queries:          " + 
                            div.computeNumberQueriesSpecializeForReplica()
                    );
            
            Rt.p(" TOTAL COST(INUM): " + totalCostBIP);
            Rt.p(" NODE IMBALANCE: " + div.getNodeImbalance());
            Rt.p(" PREDICTABILITY IMBALANCE: " + div.getQueryImbalance());   
            Rt.p(" FAILURE IMBALANCE: " + div.getFailureImbalance());
            // check for node balance constraint
            /*
            div.setUpperReplicaCost(totalCostBIP * 1.1 / nReplicas);
            output = div.solve();
            Rt.p(logger.toString());
            if (output != null)
                Rt.p(" COST OF NODE-BALANCE CONSTRAINT " + div.getObjValue()
                        + " NODE IMBALANCE: " + div.getNodeImbalance()
                        + " NEW RATIO: " + (div.getObjValue() / totalCostBIP));
            */
            return totalCostBIP;
            
        } else {
            Rt.p(" NO SOLUTION ");
            return -1;
        }
    }
    
    /**
     * Run the BIP with the parameters set by other functions
     * @throws Exception
     */
    public static double testDivSimplify(int _n, double _B, boolean isOnTheFly) throws Exception
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
        div.setCommunicatingInumOnTheFly(isOnTheFly);
        
        IndexTuningOutput output = div.solve();
        Rt.p(logger.toString());
        
        if (isExportToFile)
            div.exportCplexToFile(en.getWorkloadsFoldername() + "/test.lp");
        
        if (output != null) {
            divConf = (DivConfiguration) output;
            double costDB2 = getDB2CostDivConf();
            Rt.p(" n = " + _n + " B = " + _B
                    + " cost in INUM = "
                    + div.getObjValue());
            Rt.p(" cost in DB2 = " + costDB2);
            Rt.p(" RATIO = " + (costDB2 / div.getObjValue()));
            
            return costDB2;
        }
        else {
            Rt.p(" NO SOLUTION ");
            return -1;
        }
    }
    
    
    /**
     * Call UNIF
     */
    public static List<Double> testUniformSimplify(List<Integer> listNReplicas, double B1) 
            throws Exception
    {   
        B = B1;
        nReplicas = 1;
        loadfactor = 1;
        
        double queryCost = testDivSimplify(1, B, false);
        Rt.p("----------------------------------------");
        Rt.p(" DIV-UNIF-BIP, # replicas = " + nReplicas
                            + ", load factor = " + loadfactor
                            + ", space = " + B
                            + " COST = " + queryCost);
        List<Double> totalCost = new ArrayList<Double>();
        for (int i = 0; i < listNReplicas.size(); i++)
            totalCost.add(queryCost);
        
        return totalCost;
    }
    
    /**
     * Call UNIF
     */
    public static double testUniform(int n, double B1) throws Exception
    {   
        B = B1;
        nReplicas = 1;
        loadfactor = 1;
        
        Rt.p("----------------------------------------");
        Rt.p(" DIV-UNIF-BIP, # replicas = " + nReplicas
                            + ", load factor = " + loadfactor
                            + ", space = " + B);
        
        testDiv(1, B, false);
        
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
        // UNIF for the case with more than one replica
        Rt.p("----------------------------------------");
        nReplicas = n;
        totalCostUniform = queryCost + updateCost * nReplicas;            
        Rt.p(" DIV-UNIF, # replicas: " + nReplicas + "\n"
                + " TOTAL COST: " + totalCostUniform + "\n"
                + " QUERY cost: " + queryCost + "\n"
                + " UPDATE cost: " + (updateCost * nReplicas) + "\n"
                + " ---- Detailed UPDATE cost (ONE Replica): \n" 
                + "         - query shell & update indexes: " +
                div.getUpdateCostFromCplex() + "\n"
            );
               
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
        Rt.p(" DB2 COST----------------------------------------");
        totalCostUniform = queryCost + updateCost * nReplicas;            
        Rt.p(" DIV-UNIF, # replicas: " + nReplicas + "\n"
                + " TOTAL COST: " + totalCostUniform + "\n"
                + " UPDATE COST: " + updateCost + "\n"
                + " QUERY cost: " + queryCost + "\n"
        );
        
        Rt.p("----------------------------------------");
        return totalCostUniform;
    }
    
    
    /**
     * Compute TotalCost() of a divergent configuration 
     * in terms of DB2 units
     * 
     */
    protected static double getDB2CostDivConf() throws Exception
    {   
        double queryCostDB2 = 0.0;
        
        
        for (SQLStatement sql : workload) {
            List<Double> costs = computeQueryCostsDB2(sql, divConf);
            Collections.sort(costs);
            
            for (int k = 0; k < loadfactor; k++)
                queryCostDB2 += costs.get(k) / loadfactor;
        }
        
        Rt.p("------------ DB2 -------------\n"
                           + " QUERY cost: " + queryCostDB2 + "\n"
                           );
        
        return (queryCostDB2);
    }
    
    /**
     * Run the instance of the problem five times and 
     * get the average imbalance factors
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
            testDiv(nReplicas, B, false);
            avgImbalanceQuery += div.getQueryImbalance();
            avgImbalanceReplica += div.getNodeImbalance();
            avgNodeFailure += div.getFailureImbalance();
        }
        
        
        Rt.p("================== Number of replicas: " +  nReplicas
                            + " AVG Imbalance QUERY: "
                            + (double) avgImbalanceQuery / numRuns);
        Rt.p("================== Number of replicas: " +  nReplicas
                            + " AVG Imbalance REPLICA: "
                            + (double) avgImbalanceReplica / numRuns);
        
        Rt.p("================== Number of replicas: " +  nReplicas
                + " AVG NODE failure: "
                + (double) avgNodeFailure / numRuns);
    }

    
}
