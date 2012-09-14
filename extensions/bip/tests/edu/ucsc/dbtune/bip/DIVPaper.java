package edu.ucsc.dbtune.bip;

import static edu.ucsc.dbtune.DatabaseSystem.newDatabaseSystem;
import static edu.ucsc.dbtune.bip.CandidateGeneratorFunctionalTest.readCandidateIndexes;
import static edu.ucsc.dbtune.util.TestUtils.workload;
import static edu.ucsc.dbtune.workload.SQLCategory.DELETE;
import static edu.ucsc.dbtune.workload.SQLCategory.INSERT;
import static edu.ucsc.dbtune.workload.SQLCategory.SELECT;
import static edu.ucsc.dbtune.workload.SQLCategory.UPDATE;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import edu.ucsc.dbtune.bip.div.DivBIP;
import edu.ucsc.dbtune.bip.div.DivConfiguration;
import edu.ucsc.dbtune.bip.util.LatexGenerator;
import edu.ucsc.dbtune.bip.util.LogListener;
import edu.ucsc.dbtune.bip.util.LatexGenerator.Plot;
import edu.ucsc.dbtune.divgdesign.CoPhyDivgDesignFunctionalTest;
import edu.ucsc.dbtune.optimizer.InumOptimizer;
import edu.ucsc.dbtune.util.Environment;
import edu.ucsc.dbtune.util.GnuPlotLine;
import edu.ucsc.dbtune.util.Rt;
import edu.ucsc.dbtune.workload.SQLStatement;
import edu.ucsc.dbtune.workload.Workload;

/**
 * Run experiments presented in the DIV-paper
 * 
 * @author Quoc Trung Tran
 *
 */
public class DIVPaper extends DivTestSetting
{   
    static File outputDir;
    static File figsDir;
    static File latexFile;
    static List<Plot> plots;
    static String plotName;
    static String xname;
    static String yname;
    
    
    static DivConfiguration initialConf;
    static int nDeploys;
    
    /**
     *
     * Generate paper results
     */
    @Test
    public void main() throws Exception
    {   
        String dbNames[] = {"test"}; 
        String workloadNames[] = {"online-benchmark-100"};
        
        outputDir = new File("/tmp/div/");        
        if (!outputDir.exists())
            outputDir.mkdir();
        figsDir = new File(outputDir, "figs");
        if (!figsDir.exists())
            figsDir.mkdir();
        
        latexFile = new File(outputDir, "div.tex");
        plots = new ArrayList<Plot>();
        
        // experiment for DIV equivalent to BIP
        //for (int i = 0; i < dbNames.length; i++) 
          //  experimentDivEquiBip(dbNames[i], workloadNames[i]);
        
        for (int i = 0; i < dbNames.length; i++) 
            experimentElasticity(dbNames[i], workloadNames[i]);
        
        LatexGenerator.generateLatex(latexFile, outputDir, plots);
    }
    
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
        reConfigurationCost = divConf.transitionCost(initialConf, false);
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
    
    public static void experimentDivEquiBip(String dbName, String workloadName)
            throws Exception
    {
        // get database instances, candidate indexes
        getEnvironmentParameters(dbName, workloadName);
        
        // get parameters
        setParameters();
                
        // regenerate candidate indexes for DIVGDESIGN
        CoPhyDivgDesignFunctionalTest.forceGeneratingCandidates();
        
        String[] competitors = {"DIV-BIP", "UNIF"};
        int numX;
        int nReplica;
        double ratio;
                
        // 1. Varying number of replicas
        Rt.p(" ---- VARYING number of replicas");
        int counter = 0;
        for (double B : listBudgets) {
            counter++;
            
            numX = listNumberReplicas.size();
            double[] xtics = new double[numX];
            String[] xaxis = new String[numX];
            List<Point> points = new ArrayList<Point>();
            
            for (int i = 0; i < numX; i++) {
                nReplica = listNumberReplicas.get(i);
                xtics[i] = i;
                xaxis[i] = Integer.toString(nReplica);
                // call testDiv
                points.add(new Point(xtics[i], DivBIPFunctionalTest.testDiv(nReplica, B, false)));
                points.add(new Point(xtics[i], DivBIPFunctionalTest.testUniform(nReplica, B)));
                //points.add(new Point(xtics[i], CoPhyDivgDesignFunctionalTest.testDivgDesign(nReplica, B)));
                
            }
            
            ratio = (double) B / Math.pow(2, 30) / 10;
            plotName = dbName + "_" + workloadName + "_space_" + Double.toString(ratio) + "x";
            xname = "# replicas";
            yname = "TotalCost";
         
            drawLineGnuPlot(plotName, xname, yname, xaxis, xtics, 
                    competitors, figsDir, points);
            
            plots.add(new Plot("figs/" + plotName, 
                    " Database = " + dbName + " workload = " + workloadName +
                    " Varying \\# replicas, B = " + Double.toString(ratio) + "x", 0.5));
            
        }
        
        double B;
        // 2. Varying space budget        
        Rt.p(" ---- VARYING space budgets");
        for (int n : listNumberReplicas) {            
            nReplica = n;
            numX = listBudgets.size();
            double[] xtics = new double[numX];
            String[] xaxis = new String[numX];
            List<Point> points = new ArrayList<Point>();
            
            for (int i = 0; i < numX; i++) {
                B = listBudgets.get(i);
                xtics[i] = i;
                ratio = (double) B / Math.pow(2, 30) / 10;
                xaxis[i] = Double.toString(ratio) + "x";
                // call testDiv
                points.add(new Point(xtics[i], DivBIPFunctionalTest.testDiv(nReplica, B, false)));
                points.add(new Point(xtics[i], DivBIPFunctionalTest.testUniform(nReplica, B)));
                //points.add(new Point(xtics[i], CoPhyDivgDesignFunctionalTest.testDivgDesign(nReplica, B)));
            }
            
            plotName = dbName + "_" + workloadName + "_nreplica_" + nReplica;
            xname = "Space budget";
            yname = "TotalCost";
         
            drawLineGnuPlot(plotName, xname, yname, xaxis, xtics, 
                    competitors, figsDir, points);
            
            plots.add(new Plot("figs/" + plotName, 
                    " Database = " + dbName + " workload = " + workloadName +
                    " Varying space budget, n = " + nReplica, 0.5));
        }
        
    }
    
    /**
     * The experiments to study imbalance constraints
     * @throws Exception 
     *
    public static void exptImbalance(String dbName, String workloadName,
                                     String imbalanceType, double[] factors) throws Exception
    {
        String plotName;
        String xname;
        String yname;
        double cost, costUnif = 0.0, ratio;
        double factor;
        String[] competitors = {"DIV-BIP", "UNIF"};
        int numX;
        
        numX = factors.length;
        double[] xtics = new double[numX];
        String[] xaxis = new String[numX];
        List<Point> points = new ArrayList<Point>();
        List<DivConstraint> constraints = new ArrayList<DivConstraint>();
        
        // TODO: derive costUnif
        for (int i = 0; i < numX; i++) {
            factor = factors[i];
            xtics[i] = i;
            xaxis[i] = Double.toString(factor);

            constraints = new ArrayList<DivConstraint>();
            constraints.add(new DivConstraint(imbalanceType, factor));
            cost = ConstraintDivBIPFunctionalTest.runConstraintBIP
                    (new ConstraintDivBIP(constraints, false));
            points.add(new Point(xtics[i], cost));
            points.add(new Point(xtics[i], costUnif));
        }
        ratio = (double) B / Math.pow(2, 30) / 10;
        plotName = dbName + "_" + workloadName + "_space_" + Double.toString(ratio) + "x";
        xname = "# replicas";
        yname = "TotalCost";
     
        drawLineGnuPlot(plotName, xname, yname, xaxis, xtics, 
                competitors, figsDir, points);
        
        plots.add(new Plot("figs/" + plotName, 
                " Database = " + dbName + " workload = " + workloadName +
                imbalanceType + " , B = " + Double.toString(ratio) + "x", 0.5));
    }
    */
    
    /**
     * Retrieve the environment parameters set in {@code dbtune.cfg} file
     * 
     * @throws Exception
     */
    public static void getEnvironmentParameters
                    (String dbName, String workloadName) throws Exception
    {   
        en = Environment.getInstance();
        
        en.setProperty("jdbc.url", "jdbc:db2://localhost:50001/" + dbName);
        en.setProperty("username", "db2inst2");
        en.setProperty("password", "db2inst1admin");
        en.setProperty("workloads.dir", "resources/workloads/db2/" + workloadName);
        
        db = newDatabaseSystem(en);        
        io = db.getOptimizer();
        
        if (!(io instanceof InumOptimizer))
            return;
        
        folder = en.getWorkloadsFoldername();
        
        // get workload and candidates
        workload = workload(folder);
        candidates = readCandidateIndexes();
        Rt.p("DIVpaper: # statements in the workload: " + workload.size()
                + " # candidates in the workload: " + candidates.size()
                + " workload folder: " + folder);
    }
    
    
    /**
     * Set common parameter (e.g., workload, number of replicas, etc. )
     * 
     * @throws Exception
     */
    public static void setParameters() throws Exception
    {  
        fUpdate = 1;
        fQuery = 1;
        sf = 15000;
        
        for (SQLStatement sql : workload)
            if (sql.getSQLCategory().isSame(INSERT)) {
                
                // for TPCH workload
                if (sql.getSQL().contains("orders")) 
                    sql.setStatementWeight(sf);
                else if (sql.getSQL().contains("lineitem"))
                    sql.setStatementWeight(sf * 3.5);
            }   
            else if (sql.getSQLCategory().isSame(DELETE))
                sql.setStatementWeight(sf);            
            else if (sql.getSQLCategory().isSame(SELECT))
                sql.setStatementWeight(fQuery);
            else if (sql.getSQLCategory().isSame(UPDATE))
                sql.setStatementWeight(fUpdate);
        
        // debugging purpose
        isExportToFile = true;
        isTestCost = false;
        isShowRecommendation = true;        
        isGetAverage = false;
        isDB2Cost = false;
        isAllImbalanceConstraint = false;
        
        div = new DivBIP();
        
        // space budget
        double tenGB = 10 * Math.pow(2, 30);
        listBudgets = new ArrayList<Double>();
        for (int i = -2; i <= 0; i++)
            listBudgets.add(tenGB * Math.pow(2, i));
        listBudgets.add(5 * tenGB);
        
        // number of replicas
        listNumberReplicas = new ArrayList<Integer>();
        for (int i = 2; i <= 5; i++)
            listNumberReplicas.add(i);
        
        // default values of B, nreplica, and loadfactor
        B = listBudgets.get(0);
        nReplicas = listNumberReplicas.get(0);
        loadfactor = (int) Math.ceil( (double) nReplicas / 2);
        
        // imbalance factors
        double t1[] = {2, 1.5, 1.05};
        double t2[] = {0.8, 0.7, 0.6};
        queryImbalances = new ArrayList<Double>();
        nodeImbalances = new ArrayList<Double>();
        failureImbalances = new ArrayList<Double>();
        for (int i = 0; i < t1.length; i++) {
            queryImbalances.add(t1[i]);
            nodeImbalances.add(t1[i]);
            failureImbalances.add(t2[i]);
        }
    }
    
    /*
    public static void drawGnuBar() throws IOException
    {
        File figsDir=new File("/tmp/gnutest");
        figsDir.mkdir();
        GnuPlotBar plot = new GnuPlotBar(figsDir, "test", "x", "y");
        int[] x={2,3,4};
        plot.setXtics(x);
        String[] plotNames={"testset1","testset2"};
        plot.setPlotNames(plotNames);
        plot.add(1, 3000000);
        plot.add(1, 5);
        plot.addLine();
        plot.add(1, 13);
        plot.add(1, 15);
        plot.addLine();
        plot.add(1, 23);
        plot.add(1, 25);
        plot.addLine();
        plot.finish();
        System.exit(0);
    }
    */
    
    public static void drawLineGnuPlot(String plotName, String xname, String yname,
            String[] xaxis, double[] xtics, String[] competitors, 
            File figsDir, List<Point> points) 
            throws IOException
    {  
        figsDir.mkdir();
        GnuPlotLine plot = new GnuPlotLine(figsDir, plotName, xname, yname);
        plot.setXtics(xaxis, xtics);
        plot.setPlotNames(competitors);

        int numCompetitors = competitors.length;
        for (int i = 0; i < points.size(); i ++) {
            if (i > 0 && i % numCompetitors == 0)
                plot.addLine();
            
            plot.add(points.get(i).xaxis, points.get(i).yaxis);
        }
        plot.addLine();
        plot.finish();
    }
    
    static class Point
    {
        double xaxis;
        double yaxis;
        
        public Point(double x, double y)
        {
            xaxis = x;
            yaxis = y;
        }
    }
    
}
