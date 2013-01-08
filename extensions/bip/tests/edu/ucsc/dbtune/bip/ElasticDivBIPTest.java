package edu.ucsc.dbtune.bip;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;


import org.junit.Test;

import edu.ucsc.dbtune.bip.core.QueryPlanDesc;
import edu.ucsc.dbtune.bip.div.DivConfiguration;
import edu.ucsc.dbtune.bip.div.ElasticDivBIP;
import edu.ucsc.dbtune.bip.util.LatexGenerator;
import edu.ucsc.dbtune.bip.util.LogListener;
import edu.ucsc.dbtune.optimizer.InumOptimizer;
import edu.ucsc.dbtune.optimizer.Optimizer;
import edu.ucsc.dbtune.util.Rt;
import edu.ucsc.dbtune.workload.SQLStatement;
import edu.ucsc.dbtune.workload.Workload;


public class ElasticDivBIPTest extends AdaptiveDivBIPTest 
{   
    private static int nDeploys;
    
    @Test
    public void main() throws Exception
    {   
        // 1. initialize file locations & necessary 
        // data structures
        initialize();
        
        // get database instances, candidate indexes
        getEnvironmentParameters();
        setParameters();
        
        // set control knobs
        controlKnobs();
        
        // prepare workload
        prepareWorkload();
        
        initializeOnlineObject();
        runElasticityM();
        // move this functionality to DivPaper later
        LatexGenerator.generateLatex(latexFile, outputDir, plots);
    }
    
    /**
     * Set up elasticity experiments
     * Varying the multiplicity
     * @throws Excpetion
     */
    protected static void runElasticityM() throws Exception
    {
        ElasticPaperEntry entry = new ElasticPaperEntry();
        
        // these can come from the list of ticks
        // from the online expt.
        // for now: they are hard-coded
        int endInvestigation = 211;
        int startInvestigation = endInvestigation - en.getWindowDuration() + 1;
        
        // get initial configuration
        // the first phase, including 200 queries
        getInitialConfiguration(0, 199);
        
        // set up the new workload
        List<SQLStatement> sqls = new ArrayList<SQLStatement>();
        for (int i = startInvestigation; i <= endInvestigation; i++)
            sqls.add(wlOnline.get(i));
        workload = new Workload(sqls);
        // compute the cost of current configuration
        
        entry.currentCost = onlineDiv.computeINUMCost(startInvestigation, endInvestigation,
                                        initialConf);
        Rt.p(" current cost = " + entry.currentCost);
        List<QueryPlanDesc> descs = onlineDiv.getQueryPlanDescs(startInvestigation, endInvestigation);
        
        // 2. Get the total deployment cost
        double reConfigurationCost = 0.0;
        DivBIPFunctionalTest.testDiv(nReplicas, B, descs);
        reConfigurationCost = divConf.transitionCost(initialConf, true);
        Rt.p("Reconfiguration costs: " + reConfigurationCost);
        
        // derive the reconfiguration costs
        // base mat. cost
        double baseMatCost = reConfigurationCost 
                                * Math.pow(2,  -3);
        for (int i = 1; i <= 5; i++) 
            entry.listReconfiguationCosts.add(baseMatCost * i);
        
        
        for (int m = 1; m <= 3; m++) {
            LogListener logger = LogListener.getInstance();
            // tricky
            entry.listNumberReplicas.add(m);
            entry.mapReplicaCosts.put(m, testElasticityM
                                 (initialConf, entry.listReconfiguationCosts
                                         , m, logger, descs));
            entry.totalTime += logger.getTotalRunningTime();
            
            elasticFile = new File(rawDataDir, wlName + "_" + ELASTIC_FILE);
            elasticFile.delete();
            elasticFile = new File(rawDataDir, wlName + "_" + ELASTIC_FILE);
            elasticFile.createNewFile();
            // serialize result
            serializeElasticResult(entry, elasticFile);
        }
    }
    
    /**
     * Set up elasticity experiments
     * @throws Excpetion
     */
    protected static void runElasticity() throws Exception
    {
        ElasticPaperEntry entry = new ElasticPaperEntry();
        
        // these can come from the list of ticks
        // from the online expt.
        // for now: they are hard-coded
        int endInvestigation = 211;
        int startInvestigation = endInvestigation - en.getWindowDuration() + 1;
        
        // get initial configuration
        // the first phase, including 200 queries
        getInitialConfiguration(0, 199);
        
        // set up the new workload
        List<SQLStatement> sqls = new ArrayList<SQLStatement>();
        for (int i = startInvestigation; i <= endInvestigation; i++)
            sqls.add(wlOnline.get(i));
        workload = new Workload(sqls);
        // compute the cost of current configuration
        
        entry.currentCost = onlineDiv.computeINUMCost(startInvestigation, endInvestigation,
                                        initialConf);
        Rt.p(" current cost = " + entry.currentCost);
        List<QueryPlanDesc> descs = onlineDiv.getQueryPlanDescs(startInvestigation, endInvestigation);
        
        // 2. Get the total deployment cost
        double reConfigurationCost = 0.0;
        DivBIPFunctionalTest.testDiv(nReplicas, B, descs);
        reConfigurationCost = divConf.transitionCost(initialConf, true);
        Rt.p("Reconfiguration costs: " + reConfigurationCost);
        
        // derive the reconfiguration costs
        // base mat. cost
        //double baseMatCost = reConfigurationCost 
          //                    * Math.pow(2,  -3);
        double baseMatCost = 17 * Math.pow(10, 6);
        for (int i = 0; i <= 4; i++) 
            entry.listReconfiguationCosts.add(baseMatCost + 
                        i * 18 * Math.pow(10, 6));
        
        
        for (nDeploys = nReplicas - 1; nDeploys <= nReplicas + 1; nDeploys++) {
            LogListener logger = LogListener.getInstance();
            entry.listNumberReplicas.add(nDeploys);
            entry.mapReplicaCosts.put(nDeploys, testElasticity
                                 (initialConf, entry.listReconfiguationCosts
                                         , nDeploys, logger, descs));
            entry.totalTime += logger.getTotalRunningTime();
            
            elasticFile = new File(rawDataDir, wlName + "_" + ELASTIC_FILE);
            elasticFile.delete();
            elasticFile = new File(rawDataDir, wlName + "_" + ELASTIC_FILE);
            elasticFile.createNewFile();
            // serialize result
            serializeElasticResult(entry, elasticFile);
        }
    }
    
    /**
     * Test the elasticity aspect
     * 
     */
    public static List<Double> testElasticity(DivConfiguration initial, 
                                        List<Double> costs,
                                        int nDeploys, LogListener logger,
                                        List<QueryPlanDesc> descs)
            throws Exception
    {   
        Optimizer io = db.getOptimizer();
        
        ElasticDivBIP elastic = new ElasticDivBIP();
        elastic.setCandidateIndexes(candidates);
        elastic.setWorkload(workload); 
        elastic.setOptimizer((InumOptimizer) io);
        elastic.setSpaceBudget(B);
        elastic.setLogListenter(logger);
        elastic.setQueryPlanDesc(descs);
        
        elastic.setInitialConfiguration(initial);
        elastic.setNumberDeployReplicas(nDeploys);
        elastic.setUpperDeployCost(costs);
        Rt.p(" workload size = " + workload.size()
                + " descs = " + descs.size());
        elastic.solve();
        Rt.p("Running time: " + logger);
        return elastic.getTotalCosts();
    }
    
    /**
     * Test the elasticity aspect
     * 
     */
    public static List<Double> testElasticityM(DivConfiguration initial, 
                                        List<Double> costs,
                                        int m, LogListener logger,
                                        List<QueryPlanDesc> descs)
            throws Exception
    {   
        Optimizer io = db.getOptimizer();
        
        ElasticDivBIP elastic = new ElasticDivBIP();
        elastic.setCandidateIndexes(candidates);
        elastic.setWorkload(workload); 
        elastic.setOptimizer((InumOptimizer) io);
        elastic.setSpaceBudget(B);
        elastic.setLogListenter(logger);
        elastic.setQueryPlanDesc(descs);
        
        
        elastic.setInitialConfiguration(initial);
        elastic.setNumberDeployReplicas(nReplicas);
        elastic.setUpperDeployCost(costs);
        Rt.p(" workload size = " + workload.size()
                + " descs = " + descs.size());
        // reset the load balancing factor
        elastic.setLoadBalanceFactor(m);
        elastic.solve();
        Rt.p("Running time: " + logger);
        return elastic.getTotalCosts();
    }
}
