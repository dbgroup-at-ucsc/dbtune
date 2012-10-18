package edu.ucsc.dbtune.bip;

import java.io.File;
import java.util.ArrayList;
import java.util.List;


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
        runElasticity();
        // move this functionality to DivPaper later
        LatexGenerator.generateLatex(latexFile, outputDir, plots);
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
        for (int i = -4; i <= 0; i++) 
            entry.listReconfiguationCosts.add(reConfigurationCost 
                    * Math.pow(2,  i));
        
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
}
