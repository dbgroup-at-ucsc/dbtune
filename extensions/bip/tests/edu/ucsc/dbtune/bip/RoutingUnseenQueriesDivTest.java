package edu.ucsc.dbtune.bip;


import static edu.ucsc.dbtune.workload.SQLCategory.SELECT;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.junit.Test;

import edu.ucsc.dbtune.bip.core.IndexTuningOutput;
import edu.ucsc.dbtune.bip.core.QueryPlanDesc;
import edu.ucsc.dbtune.bip.div.DivConfiguration;
import edu.ucsc.dbtune.bip.div.RoutingDivergent;
import edu.ucsc.dbtune.bip.util.LogListener;
import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.optimizer.InumOptimizer;
import edu.ucsc.dbtune.optimizer.Optimizer;
import edu.ucsc.dbtune.util.Rt;
import edu.ucsc.dbtune.workload.SQLStatement;
import edu.ucsc.dbtune.workload.Workload;

public class RoutingUnseenQueriesDivTest extends DIVPaper 
{
    public static String fileName;
    protected static List<QueryPlanDesc> queryPlanDescs;
    protected static List<QueryPlanDesc> descTrainings;
    protected static List<QueryPlanDesc> descTestings;
    
    protected static Workload wlTraining;
    protected static Workload wlTesting;
    protected static Workload wlTotal;
    
    protected static RoutingUnseenQueriesEntry entry;
    protected static List<RoutingUnseenQueriesEntry> entries;
    
    @Test
    public void main() throws Exception
    {
        // 1. initialize file locations & necessary 
        // data structures
        initialize();
        
        // get parameters
        getEnvironmentParameters();        
        setParameters();
        //fileName = wlName + "_" + DIV_DETAIL_DB2_FILE;
        
        // experiment for DIV equivalent to BIP    
        runRoutingDivBIP();
        
        // not to draw graph
        resetParameterNotDrawingGraph();
    }
    
    /**
     * Call DIVBIP for each pair of replicas and budgets 
     * 
     * 
     * @throws Exception
     */
    public static void runRoutingDivBIP() throws Exception
    {   
        double totalCostBestCase, totalCostSplitting, totalCostUNIF;
        
        List<SQLStatement> sqlStatements = new ArrayList<SQLStatement>();
        for (SQLStatement sql : workload)
            sqlStatements.add(sql);
        wlTotal = new Workload(sqlStatements);
        
        // 1. Best case scenarios
        LogListener logger = LogListener.getInstance();
        WorkloadCostDetail wc = DivBIPFunctionalTest.testDivSimplify(nReplicas, B, 
                                false, logger);
        totalCostBestCase = wc.totalCost;
        queryPlanDescs = div.getQueryPlanDescs();
        
        // 2. split into training and testing data set
        int maxRound = 5;
        int id;
        int countTraining;
        // store result
        entries = new ArrayList<RoutingUnseenQueriesEntry>();
        fileName = wlName + "_" + ROUTING_UNSEEN_QUERY_DB2_FILE;
        
        for (int round = 0; round < maxRound; round++) {
            entry = new RoutingUnseenQueriesEntry();
            
            Rt.p(" ---- Round " + round + "-------------");
            
            List<Integer> positions = new ArrayList<Integer>();
            for (int i = 0; i < wlTotal.size(); i++)
                positions.add(i);
            
            Collections.shuffle(positions);
            countTraining = (int) (wlTotal.size() * 3 / 4);
            
            List<SQLStatement> trainings = new ArrayList<SQLStatement>();
            List<SQLStatement> testings = new ArrayList<SQLStatement>();
            
            descTrainings = new ArrayList<QueryPlanDesc>();
            descTestings = new ArrayList<QueryPlanDesc>();
            for (int i = 0; i < countTraining; i++) {
                id = positions.get(i);
                trainings.add(wlTotal.get(id));
                descTrainings.add(queryPlanDescs.get(id));
            }
            
            for (int i = countTraining; i < wlTotal.size(); i++) {
                id = positions.get(i);
                testings.add(wlTotal.get(id));
                descTestings.add(queryPlanDescs.get(id));
            }
             
            wlTraining = new Workload(trainings);
            wlTesting = new Workload(testings);
            
            totalCostSplitting = runSplittingDiv();
            
            // Call UNIF? ----------
            totalCostUNIF = runUNIF();
            // ------------------------------------------------------------
            
            Rt.p(" Total cost BEST CASE = " + totalCostBestCase);
            Rt.p(" Total cost SPLITTING = " + totalCostSplitting);
            Rt.p(" BEST CASE / SPLITTING = " + (totalCostBestCase / totalCostSplitting));
            Rt.p(" Total cost UNIF = " + totalCostUNIF);
            Rt.p(" RATIO SPLITTING / UNIF = " + (totalCostSplitting / totalCostUNIF));
        
            // TODO: recording the result
            entry.costUNIF = totalCostUNIF;
            entry.costBestCase = totalCostBestCase;
            entries.add(entry);
            storeResult();
        }
    }
    
    /**
     * Store the results into files
     * 
     * @throws Exception
     */
    protected static void storeResult() throws Exception
    {
        // 2. store in the file
        unseenQueriesFile = new File(rawDataDir, fileName);
        unseenQueriesFile.delete();
        unseenQueriesFile = new File(rawDataDir, fileName);
        unseenQueriesFile.createNewFile();
        
        // store in the serialize file
        ObjectOutputStream write;
        Rt.p("Store in file = " + unseenQueriesFile.getName());
        try {
            FileOutputStream fileOut = new FileOutputStream(unseenQueriesFile, false);
            write = new ObjectOutputStream(fileOut);
            write.writeObject(entries);
            write.close();
            fileOut.close();
        } catch(IOException e) {
            throw new SQLException(e);
        }
    }
    
    
    /**
     * Run with the training, and route queries in the testing workload
     * 
     * @param wlTraining
     *      The training workload (use to derive the divergent design)
     * @param wlTesting
     *      The testing queries that will be routed based on the training
     *      workload 
     * @return
     *      The total cost of running both the training and testing
     *      workload
     * @throws Exception
     */
    protected static double runSplittingDiv() throws Exception
    {
        double totalCostSeen, totalCostUnseen;
        RoutingDivergent div = null;
        
        // Assign training workload --------------------
        workload = wlTraining;
        LogListener logger = LogListener.getInstance();
        div = new RoutingDivergent(-1.0, 0, 0);
        loadfactor = (int) Math.ceil( (double) nReplicas / 2);
        totalCostSeen = -1;
        Optimizer io = db.getOptimizer();
        div.setCandidateIndexes(candidates);
        div.setWorkload(workload); 
        div.setOptimizer((InumOptimizer) io);
        div.setNumberReplicas(nReplicas);
        div.setLoadBalanceFactor(loadfactor);
        div.setSpaceBudget(B);
        div.setLogListenter(logger);
        div.setQueryPlanDesc(descTrainings);
        
        IndexTuningOutput output = div.solve();
        Rt.p(logger.toString());
        
        if (output != null) {
            divConf = (DivConfiguration) output;
            Rt.p(divConf.indexAtReplicaInfo());
            
            totalCostSeen = div.computeOptimizerQueryCost(divConf)
                + div.computeOptimizerUpdateCost(divConf);
            // TODO: recording the result
            entry.divTraining = divConf;
        }
        // -----------------------------------------------
        
        // Route query
        long start = System.currentTimeMillis();
        DivConfiguration divTest = div.routeUnseenWorkload(wlTesting, descTestings);
        Rt.p(" TIME TO COMPUTE THE SIMILARITY = "
                + (System.currentTimeMillis() - start)
                + "milliseconds");
        totalCostUnseen = div.computeOptimizerQueryCost(wlTesting, descTestings, 
                                                        divTest);
        // TODO: recording the result
        entry.divTesting = divTest;
        entry.costSplitting = totalCostSeen + totalCostUnseen;
        
        Rt.p(" cost seen = " + totalCostSeen);
        Rt.p(" cost unseen = " + totalCostUnseen);
        return (totalCostSeen + totalCostUnseen);
    }
    
    /**
     * Run UNIF on the training and compute the cost on total
     * 
     * @return
     *      Total cost
     * @throws Exception
     */
    public static double runUNIF() throws Exception
    {
        double costTraining, costTesting;
        db2Advis.process(wlTraining);
        int budget = (int) (B / Math.pow(2, 20));
        Set<Index> recommendation = db2Advis.getRecommendation(budget);
        
        List<Double> costs = computeCostsDB2(wlTraining, recommendation);
        double queryCost = 0.0;
        double updateCost = 0.0;
       
        for (int i = 0; i < wlTraining.size(); i++)
            if (wlTraining.get(i).getSQLCategory().isSame(SELECT))
                queryCost += costs.get(i) * wlTraining.get(i).getStatementWeight();
            else
                updateCost += costs.get(i) * wlTraining.get(i).getStatementWeight();
        costTraining = queryCost + updateCost;
        
        queryCost = updateCost = 0.0;
        costs = computeCostsDB2(wlTesting, recommendation);
        for (int i = 0; i < wlTesting.size(); i++)
            if (wlTesting.get(i).getSQLCategory().isSame(SELECT))
                queryCost += costs.get(i) * wlTesting.get(i).getStatementWeight();
            else
                updateCost += costs.get(i) * wlTesting.get(i).getStatementWeight();
        costTesting = queryCost + updateCost;
        
        Rt.p(" UNIF cost training = " + costTraining);
        Rt.p(" UNIF cost testing = " + costTesting);
        
        return (costTraining + costTesting);
    }
}
