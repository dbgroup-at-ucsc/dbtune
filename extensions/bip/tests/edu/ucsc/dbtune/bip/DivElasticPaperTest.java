package edu.ucsc.dbtune.bip;

import java.util.ArrayList;
import java.util.List;

import edu.ucsc.dbtune.bip.DIVPaper.Point;
import edu.ucsc.dbtune.bip.util.LogListener;
import edu.ucsc.dbtune.bip.util.LatexGenerator.Plot;
import edu.ucsc.dbtune.util.Rt;
import edu.ucsc.dbtune.workload.SQLStatement;
import edu.ucsc.dbtune.workload.Workload;

public class DivElasticPaperTest extends DIVPaper 
{
    /**
     * Testing the elasticity aspect
     * @param dbName
     * @param workloadName
     * @throws Exception
     */
    public static void experimentElasticity(String dbName, String workloadName)
            throws Exception
    {
        // get database instances, candidate indexes
        getEnvironmentParameters(dbName, workloadName);
        
        // get parameters
        setParameters();
        
        // 1. Create initial configuration
        nReplicas = 3;
        B = 10 * Math.pow(2, 30);
        getInitialConfiguration();
        
        // 2. Get the total deployment cost
        double reConfigurationCost = 0.0;
        DivBIPFunctionalTest.testDiv(nReplicas, B, false);
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
                                 (initialConf, costs, nDeploys, logger));
            totalTime += logger.getTotalRunningTime();
        }
        
        for (int i = 0; i < numX; i++){
            for (int j = 0; j < competitors.length; j++){
                points.add(new Point(xtics[i], totalCostCompetitors.get(j).get(i)));
            }
        }
        
        plotName = dbName + "_" + workloadName + "elastic";
        xname = "Reconfiguration cost, each unit is " 
                    + Double.toString(reConfigurationCost);
        yname = "Total cost";
     
        drawLineGnuPlot(plotName, xname, yname, xaxis, xtics, 
                competitors, figsDir, points);
        totalTime /= 1000;
        
        plots.add(new Plot("figs/" + plotName, 
                " Database = " + dbName + ", workload = " + workloadName +
                " Elasticity, initial configuration includes the first 20 queries"
                + ". The running time to generate this graph = "
                + totalTime + " secs ", 0.5));
    }
    
    protected static void getInitialConfiguration() throws Exception
    {
        // an alternative
        //initialConf = new DivConfiguration(3, 2);
        
        List<SQLStatement> sqls = new ArrayList<SQLStatement>();
        List<SQLStatement> sqlInitials = new ArrayList<SQLStatement>();
        for (int i = 0; i < workload.size(); i++){
            sqls.add(workload.get(i));
            if (i < workload.size() / 5)
                sqlInitials.add(workload.get(i));
        }
        
        Workload wlStore = new Workload(sqls);
        workload = new Workload(sqlInitials);
        Rt.p("Initial configuration: " + workload.size());
        DivBIPFunctionalTest.testDiv(nReplicas, B, true);
        initialConf = divConf;
        
        // reset workload
        workload = wlStore;
        
    }
}
