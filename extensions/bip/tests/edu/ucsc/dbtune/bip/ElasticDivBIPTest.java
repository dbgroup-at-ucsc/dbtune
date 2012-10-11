package edu.ucsc.dbtune.bip;

import java.util.ArrayList;
import java.util.List;


import org.junit.Test;

import edu.ucsc.dbtune.bip.core.QueryPlanDesc;
import edu.ucsc.dbtune.bip.div.DivConfiguration;
import edu.ucsc.dbtune.bip.div.ElasticDivBIP;
import edu.ucsc.dbtune.bip.util.LatexGenerator;
import edu.ucsc.dbtune.bip.util.LogListener;
import edu.ucsc.dbtune.bip.util.LatexGenerator.Plot;
import edu.ucsc.dbtune.optimizer.InumOptimizer;
import edu.ucsc.dbtune.optimizer.Optimizer;
import edu.ucsc.dbtune.util.Rt;
import edu.ucsc.dbtune.workload.SQLStatement;
import edu.ucsc.dbtune.workload.Workload;


public class ElasticDivBIPFunctionalTest extends AdaptiveDivBIPTest 
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
        // these can come from the list of ticks
        // from the online expt.
        // for now: they are hard-coded
        
        int endInvestigation = 211;
        int startInvestigation = endInvestigation - en.getWindowDuration();
        
        
        // get initial configuration
        // empty configuration
        getInitialConfiguration(0, 199);
        
        // set up the new workload
        List<SQLStatement> sqls = new ArrayList<SQLStatement>();
        for (int i = startInvestigation; i <= endInvestigation; i++)
            sqls.add(wlOnline.get(i));
        workload = new Workload(sqls);
        
        List<QueryPlanDesc> descs = onlineDiv.getQueryPlanDescs(startInvestigation, endInvestigation);
        
        // 2. Get the total deployment cost
        double reConfigurationCost = 0.0;
        DivBIPFunctionalTest.testDiv(nReplicas, B, descs);
        reConfigurationCost = divConf.transitionCost(initialConf, true);
        Rt.p("Reconfiguration costs: " + reConfigurationCost);
        
        List<Double> costs = new ArrayList<Double>();
        int startExpo = -4;
        int endExpo = 0;
        int numX;
        numX = endExpo - startExpo + 1;
        double[] xtics = new double[numX];
        String[] xaxis = new String[numX];
        List<Point> points = new ArrayList<Point>();
        for (int i = startExpo; i <= endExpo; i++) {
            costs.add(reConfigurationCost * Math.pow(2,  i));
            xtics[i + numX - 1] = i + numX;
            xaxis[i + numX - 1] = Double.toString(Math.pow(2,  i)) + "x";
        }
        
        String[] competitors = {"n = 2", "n = 3", "n = 4"};
        List< List<Double> > totalCostCompetitors = new ArrayList<List<Double>>();
        double totalTime = 0.0;
        for (nDeploys = 2; nDeploys <= 4; nDeploys++) {
            LogListener logger = LogListener.getInstance();
            totalCostCompetitors.add(ElasticDivBIPFunctionalTest.testElasticity
                                 (initialConf, costs, nDeploys, logger, descs));
            totalTime += logger.getTotalRunningTime();
        }
        
        for (int i = 0; i < numX; i++){
            for (int j = 0; j < competitors.length; j++)
                points.add(new Point(xtics[i], totalCostCompetitors.get(j).get(i)));
            
        }
       
        plotName = dbName + "_" + wlName + "elastic";
        xname = "Reconfiguration cost, each unit is " 
                    + Double.toString(reConfigurationCost);
        yname = "Total cost";
     
        drawLineGnuPlot(plotName, xname, yname, xaxis, xtics, 
                competitors, figsDir, points);
        totalTime /= 1000;
        
        plots.add(new Plot("figs/" + plotName, 
                " Database = " + dbName + ", workload = " + wlName +
                " Elasticity, initial configuration includes the first 20 queries"
                + ". The running time to generate this graph = "
                + totalTime + " secs ", 0.5));
               
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
