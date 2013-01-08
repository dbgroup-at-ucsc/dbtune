package edu.ucsc.dbtune.bip;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Test;

import edu.ucsc.dbtune.bip.div.DivConfiguration;
import edu.ucsc.dbtune.bip.div.UtilConstraintBuilder;
import edu.ucsc.dbtune.bip.util.LatexGenerator;
import edu.ucsc.dbtune.bip.util.LatexGenerator.Plot;
import edu.ucsc.dbtune.util.GnuPlotLine;
import edu.ucsc.dbtune.util.HashCodeUtil;
import edu.ucsc.dbtune.util.Rt;

/**
 * Run experiments presented in the DIV-paper
 * 
 * @author Quoc Trung Tran
 *
 */
public class DIVPaper extends DivTestSetting
{   
    protected static File outputDir;
    protected static File figsDir;
    protected static File rawDataDir;
    protected static File latexFile;
    protected static List<Plot> plots;
    protected static String plotName;
    protected static String xname;
    protected static String yname;
    
    protected static DivConfiguration initialConf;
    protected static int nDeploys;
    
    protected static Map<DivPaperEntry, Double> entries;
    protected static Map<DivPaperEntryDetail, Double> detailEntries;
    
    protected static final String DIV_DB2_FILE = "div_db2.bin";
    protected static final String UNIF_DB2_FILE = "unif_db2.bin";
    protected static final String DESIGN_DB2_FILE = "design_db2.bin";
    
    protected static final String DIV_DETAIL_DB2_FILE = "div_detail_db2.bin";
    protected static final String UNIF_DETAIL_DB2_FILE = "unif_detail_db2.bin";
    protected static final String DESIGN_DETAIL_DB2_FILE = "design_detail_db2.bin";
    
    protected static final String ONLINE_FILE = "online.bin";
    protected static final String ELASTIC_FILE = "elastic.bin";
    protected static final String FAILURE_FILE = "failure.bin";
    protected static final String IMBALANCE_GREEDY_FILE = "imbalance_greedy.bin";
    
    protected static final String FAILURE_IMBALANCE_GREEDY_FILE = "failure_imbalance_greedy.bin";
    
    protected static final String DIV_UPDATE_COST_CONSTRAINT = "div_update_cost.bin";
    protected static final String ROUTING_UNSEEN_QUERY_DB2_FILE = "div_routing_unseen_queries.bin";
    
    protected static File unifFile;
    protected static File divFile;
    protected static File designFile;
    protected static File onlineFile;
    protected static File unseenQueriesFile;
    
    protected static File failureFile;
    protected static File elasticFile;
    protected static File imbalanceFile;
    protected static File failureImbalanceFile;
    
    protected static Map<DivPaperEntry, Double> mapUnif;
    protected static Map<DivPaperEntry, Double> mapDiv;
    protected static Map<DivPaperEntry, Double> mapDesign;    

    protected static Map<DivPaperEntryDetail, Double> mapUnifDetail;
    protected static Map<DivPaperEntryDetail, Double> mapDivDetail;
    protected static Map<DivPaperEntryDetail, Double> mapDesignDetail;    
    
    // change weight of update
    protected static double[] updateRatios = new double[] 
                                    {Math.pow(10, -5),
                                     Math.pow(10, -4),
                                     Math.pow(10, -3),
                                     Math.pow(10, -2)};
    
    // change weight of update
    protected static double[] constraintUpdateRatios = new double[] 
                                    {0.4, 0.6, 0.8, 1.0};
    
    protected static boolean isEquivalent = true;
    
    // draw figures
    protected static boolean isFailure = false;
    protected static boolean isImbalance = false;
    // online & elasticity
    protected static boolean isOnline = false;
    protected static boolean isElastic = false;
    
    // generate PDF file
    protected static boolean isLatex = false;
    
    // analyze index configuration
    protected static boolean isAnalyzeConfiguration = false;
    
    // draw figures
    protected static boolean isUpdateConstraint = false;
    
    // draw unseen queries
    protected static boolean isUnseeQueries = false;
    
    /**
     *
     * Generate paper results
     */
    @Test
    public void main() throws Exception
    {
        // 1. initialize file locations & necessary 
        // data structures
        initialize();        
        getEnvironmentParameters();        
        setParameters();
        
        // 2. draw graphs
        if (isEquivalent){
            // ratio
            //drawEquivalent(dbName, wlName, true, false);
            // absolute value
            drawEquivalent(dbName, wlName, false, false);
            /*
            boolean isDesign = false;
            // draw ratio with design
            drawEquivalentDetail(dbName, wlName, true, true);            
            // the absolute value without DIVGDESIGN
            drawEquivalentDetail(dbName, wlName, false, true);
            // with UNIF
            drawEquivalentDetail(dbName, wlName, false, false);
            drawEquivalentDetailChangeUpdateWeight
                    (dbName, wlName, isDesign);
                    */
        }
        
        if (isUpdateConstraint)
            drawUpdateConstraint(dbName, wlName);
        
        if (isFailure)
            drawFailure();

        if (isUnseeQueries)
            drawUnseenQueries();

        
        if (isOnline)
            drawOnline();

        if (isElastic)
            drawElastic();
        
        if (isAnalyzeConfiguration)
            analyzeDivConfiguration();
        
        isLatex = isOnline || isEquivalent || isFailure
                    || isImbalance || isElastic || isUpdateConstraint
                    || isUnseeQueries;

        if (isLatex)
            LatexGenerator.generateLatex(latexFile, outputDir, plots);
    }
    
    /**
     * Reset parameters so as not to draw any graphs
     */
    protected static void resetParameterNotDrawingGraph()
    {
        isEquivalent = false;
        isOnline = false;
        isLatex = false;
        isFailure = false;
        isImbalance = false;
        isElastic = false;
        isUpdateConstraint = false;
    }
    /**
     * Initialize the locations that stored file
     */
    public static void initialize()
    {
        outputDir = new File("/home/tqtrung/expt/div");        
        if (!outputDir.exists())
            outputDir.mkdir();
        
        figsDir = new File(outputDir, "figs");
        if (!figsDir.exists())
            figsDir.mkdir();
        
        rawDataDir = new File(outputDir, "rawdata");
        if (!rawDataDir.exists())
            rawDataDir.mkdir();
        
        latexFile = new File(outputDir, "div.tex");
        plots = new ArrayList<Plot>();
    }
    
    /**
     * Read the data from files and draw the graphs
     */
    public static void drawEquivalent(String dbName, String wlName, boolean drawRatio, 
                    boolean isDesign) 
                throws Exception
    {   
        DivPaperEntry entry;
        
        // 1. Read the result from UNIF file
        unifFile = new File(rawDataDir, wlName + "_" + UNIF_DETAIL_DB2_FILE);
        mapUnifDetail = readDivResultDetail(unifFile);
        Rt.p(" read from file = " + unifFile.getAbsolutePath());
        
        // 2. Read the result from DIV file
        divFile = new File(rawDataDir, wlName + "_" + DIV_DETAIL_DB2_FILE);
        mapDivDetail = readDivResultDetail(divFile);
        
        // 3. Read the result from Design file
        if (isDesign) {
            designFile = new File(rawDataDir, wlName + "_" + DESIGN_DETAIL_DB2_FILE);
            mapDesignDetail = readDivResultDetail(designFile);
        }
        
        String[] competitors;
        
        // 3. draw graphs
        if (isDesign){
            if (drawRatio)
                competitors = new String[] {"1 - RITA/UNIF", "1 - RITA/DIVGDESIGN"};
            else 
                competitors = new String[] {"UNIF", "DIVGDESIGN", "RITA"};
        } else {
            if (drawRatio)
                competitors = new String[] {"1 - RITA/UNIF"};
            else 
                competitors = new String[] {"UNIF", "RITA"};
        }
        int numX;
        double ratio; 
        long budget;
        /*
        // varying space budget
        for (int n : listNumberReplicas) {
            double B;
            numX = listBudgets.size();
            double[] xtics = new double[numX];
            String[] xaxis = new String[numX];
            List<Point> points = new ArrayList<Point>();
            
            for (int i = 0; i < numX; i++) {
                B = listBudgets.get(i);
                budget = convertBudgetToMB(B);
                xtics[i] = i;
                ratio = (double) B / Math.pow(2, 30) / 10;
                if (ratio > 1.0)
                    xaxis[i] = "INF";
                else
                    xaxis[i] = Double.toString(ratio) + "x";
                entry = new DivPaperEntry(dbName, wlName, n, budget, null);
                if (drawRatio)
                    addPointRatioDIVEquivBIP(xtics[i], entry, points, isDesign);
                else 
                    addPointDIVEquivBIP(xtics[i], entry, points, isDesign);
            }
            
            plotName = dbName + "_" + wlName + "_number_replica" + Integer.toString(n);
            if (drawRatio)
                plotName += "_ratio";
            else
                plotName += "_absolute";
            
            xname = "Number of replicas";
            if (drawRatio)
                yname = "TotalCost Improvement (%)";
            else 
                yname = "TotalCost";
            
            drawLineGnuPlot(plotName, xname, yname, xaxis, xtics, 
                    competitors, figsDir, points);
            
            plots.add(new Plot("figs/" + plotName, 
                    " Database = " + dbName + " workload = " + wlName +
                    " Varying space budget, n = " + n, 0.5));
        }
        */
        // varying number of replicas
        int n;
        for (double  B : listBudgets) {
           
            numX = listNumberReplicas.size();
            double[] xtics = new double[numX];
            String[] xaxis = new String[numX];
            List<Point> points = new ArrayList<Point>();
            ratio = (double) B / Math.pow(2, 30) / 10;
            budget = convertBudgetToMB (B);
            
            for (int i = 0; i < numX; i++) {
                n = listNumberReplicas.get(i);
                xtics[i] = i;
                
                xaxis[i] = Integer.toString(n);
                entry = new DivPaperEntry(dbName, wlName, n, budget, null);
                if (drawRatio)
                    addPointRatioDIVEquivBIP(xtics[i], entry, points, isDesign);
                else 
                    addPointDIVEquivBIP(xtics[i], entry, points, isDesign);
            }
            
            plotName = dbName + "_" + wlName + "_space_" + Double.toString(ratio); 
            if (drawRatio)
                plotName += "_ratio";
            else
                plotName += "_absolute";
            
            xname = "Number of replicas";
            if (drawRatio)
                yname = "TotalCost Improvement (%)";
            else 
                yname = "TotalCost";
            
            drawLineGnuPlot(plotName, xname, yname, xaxis, xtics, 
                    competitors, figsDir, points);
            
            plots.add(new Plot("figs/" + plotName, 
                    " Database = " + dbName + " workload = " + wlName +
                    " Varying number replicas, B = " + Double.toString(ratio), 0.5));
        }
    }
    
    /**
     * Read the data from files and draw the graphs
     */
    public static void drawEquivalentDetail(String dbName, String wlName, boolean drawRatio, 
                    boolean isDesign) 
                throws Exception
    {   
        DivPaperEntry entry;
        
        // 1. Read the result from UNIF file
        unifFile = new File(rawDataDir, wlName + "_" + UNIF_DETAIL_DB2_FILE);
        mapUnifDetail = readDivResultDetail(unifFile);
        Rt.p(" read from file = " + unifFile.getAbsolutePath());
        
        // 2. Read the result from DIV file
        divFile = new File(rawDataDir, wlName + "_" + DIV_DETAIL_DB2_FILE);
        mapDivDetail = readDivResultDetail(divFile);
        
        // 3. Read the result from Design file
        if (isDesign) {
            designFile = new File(rawDataDir, wlName + "_" + DESIGN_DETAIL_DB2_FILE);
            mapDesignDetail = readDivResultDetail(designFile);
        }
        
        String[] competitors;
        
        // 3. draw graphs
        if (isDesign){
            if (drawRatio)
                competitors = new String[] {"1 - RITA/UNIF", "1 - RITA/DIVGDESIGN"};
            else 
                competitors = new String[] {
                    "DESIGN-querycost",
                    "DESIGN-updatecost",
                    "RITA-querycost",
                    "RITA-updatecost"};
        } else {
            if (drawRatio)
                competitors = new String[] {"1 - RITA/UNIF"};
            else 
                competitors = new String[] {
                                            "UNIF-querycost",
                                            "UNIF-updatecost",
                                            "RITA-querycost",
                                            "RITA-updatecost"};
        }
        int numX;
        double ratio; 
        long budget;
        
        Rt.p(" is Design " + isDesign);
        // varying number of replicas
        int n;
        for (double  B : listBudgets) {
           
            numX = listNumberReplicas.size();
            double[] xtics = new double[numX];
            String[] xaxis = new String[numX];
            List<Point> points = new ArrayList<Point>();
            ratio = (double) B / Math.pow(2, 30) / 10;
            budget = convertBudgetToMB (B);
            
            for (int i = 0; i < numX; i++) {
                n = listNumberReplicas.get(i);
                xtics[i] = i;
                
                xaxis[i] = Integer.toString(n);
                entry = new DivPaperEntry(dbName, wlName, n, budget, null);
                if (drawRatio)
                    addPointRatioDIVEquivBIPDetail(xtics[i], entry, points, isDesign);
                else 
                    addPointDIVEquivBIPDetail(xtics[i], entry, points, isDesign);
            }
            
            plotName = dbName + "_" + wlName + "_space_" + Double.toString(ratio)
                    + "_" + isDesign; 
            if (drawRatio)
                plotName += "_ratio";
            else
                plotName += "_absolute";
            
            xname = "Number of replicas";
            if (drawRatio)
                yname = "TotalCost Improvement (%)";
            else 
                yname = "TotalCost";
            
            drawLineGnuPlot(plotName, xname, yname, xaxis, xtics, 
                    competitors, figsDir, points);
            
            plots.add(new Plot("figs/" + plotName, 
                    " Database = " + dbName + " workload = " + wlName +
                    " Varying number replicas, B = " + Double.toString(ratio), 0.5));
        }
    }
    
    
    /**
     * Add corresponding points into the list of points, which are 
     * displayed in GnuPlot
     * @param entry
     * @param points
     */
    protected static void addPointRatioDIVEquivBIP(double xcoordinate, DivPaperEntry entry, List<Point> points,
                            boolean isDesign)
    {
        double costDiv, costUnif, costDesign = 0.0;
        double ratioDesign = 0.0, ratioDiv;
        Rt.p("entry = " + entry);
        costDiv = mapDivDetail.get(entry);
        costUnif = mapUnifDetail.get(entry);    
        if (isDesign)
            costDesign = mapDesignDetail.get(entry);
        
        Rt.p(" entry: " + entry);
        Rt.p(" cost UNIF = " + (costUnif / Math.pow(10, 6)));
        Rt.p(" cost DIV = " + (costDiv / Math.pow(10, 6)));
        //Rt.p(" cost DESIGN = " + (costDesign / Math.pow(10, 6)));
        
        ratioDiv = 1 - (double) costDiv / costUnif;
        if (isDesign) {
            ratioDesign = 1 - (double) costDiv / costDesign;
            ratioDesign = ratioDesign * 100;
            if (ratioDesign < 0.0){
                Rt.p("watch out, entry = " + entry 
                        + ", ratioDesign = " + ratioDesign);
                ratioDesign = 0.05;
            }
        }
        ratioDiv = ratioDiv * 100;
        
        if (ratioDiv < 0.0) {
            Rt.p(" watch out, entry = " + entry
                    + ", ratio DIV = " + ratioDiv);
            ratioDiv = 0.05;  
        }
        
        points.add(new Point(xcoordinate, ratioDiv));
        if (isDesign)
            points.add(new Point(xcoordinate, ratioDesign));
    }
    
    /**
     * Add corresponding points into the list of points, which are 
     * displayed in GnuPlot
     * @param entry
     * @param points
     */ 
    protected static void addPointDIVEquivBIP(double xcoordinate, DivPaperEntry entry, 
                            List<Point> points, boolean isDesign)
    {
        double costDiv, costUnif, costDesign = 0.0;
        Rt.p(" entry = " + entry); 
        costDiv = mapDivDetail.get(entry);
        costUnif = mapUnifDetail.get(entry);
        if (isDesign)
            costDesign = mapDesignDetail.get(entry);
        
        points.add(new Point(xcoordinate, costUnif));
        if (isDesign)
            points.add(new Point(xcoordinate, costDesign));
        points.add(new Point(xcoordinate, costDiv));
    }
    
    /**
     * Read the data from files and draw the graphs
     */
    public static void drawEquivalentDetailChangeUpdateWeight
            (String dbName, String wlName, boolean isDesign) 
                throws Exception
    {   
        DivPaperEntry entry;
        String[] competitors;
        
        // 3. draw graphs
        if (isDesign){             
            competitors = new String[] {"UNIF", "DIVGDESIGN", "RITA"};
        } else {
            competitors = new String[] {
                    "UNIF-querycost",
                    "UNIF-updatecost",
                    "RITA-querycost",
                    "RITA-updatecost"};
        }
        int numX;
        double ratio; 
        long budget;
        
        // varying number of replicas
        int n;
        String prefix;
        
        numX = updateRatios.length;
        double[] xtics = new double[numX];
        String[] xaxis = new String[numX];
        List<Point> points = new ArrayList<Point>();
        ratio = (double) B / Math.pow(2, 30) / 10;
        budget = convertBudgetToMB (B);
        
        int i = 0;
        // TODO change the default value
        n = 3;
        
        for (double  w : updateRatios) {
            prefix = wlName + "_" + Double.toString(w) + "_";
            
            // 1. Read the result from UNIF file
            unifFile = new File(rawDataDir,  prefix + 
                                    UNIF_DETAIL_DB2_FILE);
            Rt.p(" read from file = " + unifFile.getAbsolutePath());
            mapUnifDetail = readDivResultDetail(unifFile);
            
            // 2. Read the result from DIV file
            divFile = new File(rawDataDir, prefix + DIV_DETAIL_DB2_FILE);
            mapDivDetail = readDivResultDetail(divFile);
            Rt.p(" read from file = " + divFile.getAbsolutePath());

            // 3. Read the result from Design file
            if (isDesign) {
                designFile = new File(rawDataDir, prefix + DESIGN_DETAIL_DB2_FILE);
                mapDesignDetail = readDivResultDetail(designFile);
                Rt.p(" read from file = " + designFile.getAbsolutePath());
            }
            
            xtics[i] = i;
            int pow = (int) Math.ceil(Math.log(w * Math.pow(10, 5)) / Math.log(10));
            xaxis[i] = "10^{" + Integer.toString(pow  - 5) + "}";
            entry = new DivPaperEntry(dbName, wlName, n, budget, null);
            addPointDIVEquivBIPDetail(xtics[i], entry, points, isDesign);
            i++;
        }
            
        plotName = dbName + "_" + wlName + "_varying_update_weight";
        plotName += "_absolute";
            
        xname = "Percentage of updates";
        yname = "TotalCost";
            
        drawLineGnuPlot(plotName, xname, yname, xaxis, xtics, 
                    competitors, figsDir, points);
            
        plots.add(new Plot("figs/" + plotName, 
                " Database = " + dbName + " workload = " + wlName +
                " Varying update weight, B = " + Double.toString(ratio), 0.5));
    
    }
    
    
    /**
     * Add corresponding points into the list of points, which are 
     * displayed in GnuPlot
     * @param entry
     * @param points
     */
    protected static void addPointRatioDIVEquivBIPDetail(double xcoordinate, DivPaperEntry entry, List<Point> points,
                            boolean isDesign)
    {
        double costDiv, costUnif, costDesign = 0.0;
        double ratioDesign = 0.0, ratioDiv;
        //Rt.p("entry = " + entry);
        costDiv = mapDivDetail.get(entry);
        costUnif = mapUnifDetail.get(entry);    
        if (isDesign)
            costDesign = mapDesignDetail.get(entry);
        
        //Rt.p(" entry: " + entry);
        //Rt.p(" cost UNIF = " + (costUnif / Math.pow(10, 6)));
        //Rt.p(" cost DIV = " + (costDiv / Math.pow(10, 6)));
        //Rt.p(" cost DESIGN = " + (costDesign / Math.pow(10, 6)));
        
        ratioDiv = 1 - (double) costDiv / costUnif;
        if (isDesign) {
            ratioDesign = 1 - (double) costDiv / costDesign;
            ratioDesign = ratioDesign * 100;
            if (ratioDesign < 0.0){
                Rt.p("watch out, entry = " + entry 
                        + ", ratioDesign = " + ratioDesign);
                ratioDesign = 0.05;
            }
        }
        ratioDiv = ratioDiv * 100;
        
        if (ratioDiv < 0.0) {
            Rt.p(" watch out, entry = " + entry
                    + ", ratio DIV = " + ratioDiv);
            ratioDiv = 0.05;  
        }
        
        points.add(new Point(xcoordinate, ratioDiv));
        if (isDesign)
            points.add(new Point(xcoordinate, ratioDesign));
    }
    
    /**
     * Add corresponding points into the list of points, which are 
     * displayed in GnuPlot
     * @param entry
     * @param points
     */ 
    protected static void addPointDIVEquivBIPDetail(double xcoordinate, DivPaperEntry entry, 
                            List<Point> points, boolean isDesign)
    {
        Map<DivPaperEntryDetail, Double> mapVal;
        if (isDesign)
            mapVal = mapDesignDetail;
        else
            mapVal = mapUnifDetail;
        
        for (Map.Entry<DivPaperEntryDetail, Double> ele : mapVal.entrySet()) {
            if (ele.getKey().equals(entry)) {
                points.add(new Point(xcoordinate, ele.getKey().queryCost));
                points.add(new Point(xcoordinate, ele.getKey().updateCost));
                //Rt.p(" query cost = " + ele.getKey().queryCost);
                //Rt.p(" update cost = " + ele.getKey().updateCost);
                break;
            }
        }
        
        // DIVBIP
        for (Map.Entry<DivPaperEntryDetail, Double> ele : mapDivDetail.entrySet()) {
            if (ele.getKey().equals(entry)) {
                points.add(new Point(xcoordinate, ele.getKey().queryCost));
                points.add(new Point(xcoordinate, ele.getKey().updateCost));
                
                //Rt.p(" DIV query cost = " + ele.getKey().queryCost);
                //Rt.p(" DIV update cost = " + ele.getKey().updateCost);
                break;
            }
        }
            
    }
    
    /**
     * Add corresponding points into the list of points, which are 
     * displayed in GnuPlot
     * @param entry
     * @param points
     */ 
    protected static void addPointDIVEquivBIPQueryCostDetail
                        (double xcoordinate, DivPaperEntry entry, 
                            List<Point> points)
    {
        
        for (Map.Entry<DivPaperEntryDetail, Double> ele : mapUnifDetail.entrySet()) {
            if (ele.getKey().equals(entry)) {
                points.add(new Point(xcoordinate, ele.getKey().queryCost));
                break;
            }
        }
        
        // DIVBIP
        for (Map.Entry<DivPaperEntryDetail, Double> ele : mapDivDetail.entrySet()) {
            if (ele.getKey().equals(entry)) {
                points.add(new Point(xcoordinate, ele.getKey().queryCost));
                Rt.p(" query cost = "
                        + ele.getKey().queryCost);
                break;
            }
        }   
    }
    
    /**
     * Read the data from files and draw the graphs
     */
    public static void drawUpdateConstraint (String dbName, String wlName) 
                throws Exception
    {   
        DivPaperEntry entry;
        String[] competitors;
        
        // 3. draw graphs
        competitors = new String[] {"UNIF", "RITA"};
        
        int numX;
        double ratio; 
        long budget;
        String prefix;
        
        numX = constraintUpdateRatios.length;
        double[] xtics = new double[numX];
        String[] xaxis = new String[numX];
        List<Point> points = new ArrayList<Point>();
        ratio = (double) B / Math.pow(2, 30) / 10;
        budget = convertBudgetToMB (B);
        
        int i = 0;
        int n = nReplicas;
        
        // 1. Read the result from UNIF file
        unifFile = new File(rawDataDir, wlName + "_" + UNIF_DETAIL_DB2_FILE);
        Rt.p(" read from file = " + unifFile.getAbsolutePath());
        mapUnifDetail = readDivResultDetail(unifFile);
        
        for (double  w : constraintUpdateRatios) {
            prefix = wlName + "_" + Double.toString(w) + "_";
            
            // 2. Read the result from DIV file
            divFile = new File(rawDataDir, prefix + DIV_UPDATE_COST_CONSTRAINT);
            mapDivDetail = readDivResultDetail(divFile);
            Rt.p(" read from file = " + divFile.getAbsolutePath());
            
            xtics[i] = i;
            xaxis[i] = Double.toString(w);
            entry = new DivPaperEntry(dbName, wlName, n, budget, null);
            addPointDIVEquivBIPQueryCostDetail(xtics[i], entry, points);
            i++;
        }
            
        plotName = dbName + "_" + wlName + "_update_constraint";
        plotName += "_absolute";
            
        xname = "Ratio of update cost of RITA over UNIF";
        yname = "QueryCost";
            
        drawLineGnuPlot(plotName, xname, yname, xaxis, xtics, 
                    competitors, figsDir, points);
            
        plots.add(new Plot("figs/" + plotName, 
                " Database = " + dbName + " workload = " + wlName +
                " Varying upper bound update cost, B = " + Double.toString(ratio), 0.5));
    
    }
    
    
    /***************************************************
     * 
     * Draw online graph
     * 
     * 
     ***************************************************/
    public static void drawOnline() throws Exception
    {   
        int windowDuration = en.getWindowDuration();
        onlineFile = new File(rawDataDir, wlName + "_" + ONLINE_FILE
                + "_windows_" + windowDuration);
        OnlinePaperEntry entry = readOnlineResult(onlineFile);
        
        String[] competitors = {"1 - OPT/INITIAL"};
        
        int numX = entry.getListInitial().size();
        // the last two for the running time
        double cost;
        double[] xtics = new double[numX];
        String[] xaxis = new String[numX];
        List<Point> points = new ArrayList<Point>();
        Map<Integer, Integer> ticks = new HashMap<Integer, Integer>();
        for (int id : entry.getReconfigurationStmts())
            ticks.put(id, id);
        
        Rt.p(" ticks: " + ticks);
        
        double ratio;
            
        for (int i = 0; i < numX; i++) {
            cost = entry.getListOpt().get(i);
            
            if (i == 0 || ticks.containsKey(i))
                xaxis[i] = Integer.toString(i);
            else
                xaxis[i] = "null"; // NOT mark this
            xtics[i] = i;
            ratio = 1 - cost / entry.getListInitial().get(i);
            if (ratio < 0)
                ratio = 0.0;
            
            ratio = ratio * 100;
            points.add(new Point(i, ratio));
        }
        
        plotName = dbName + "_" + wlName + "_online_windows_" + windowDuration;
        xname = "Query";
        yname = "Expected Cost Improvement (%)";
        
        drawLineGnuPlot(plotName, xname, yname, xaxis, xtics, 
                competitors, figsDir, points);
        
        double avgInum = entry.getTimeInum() / numX / 1000;
        double avgBip = entry.getTimeBIP() / numX / 1000;
        
        plots.add(new Plot("figs/" + plotName,  
                " ONLINE, space = 0.5x, n = 3"
                + "time BIP = " + entry.getTimeBIP()
                + " window duration =  " + entry.getWindowDuration()
                + " AVG Inum = " + avgInum  + "(secs)"
                + " AVG BIP = " + avgBip  + "(secs)"
                + " AVG one query = " + (avgInum + avgBip) + "(secs)"
                , 0.5));
        
        Rt.p("reconfiguration: " + entry.getReconfigurationStmts());
        Rt.p(" AVG BIP = " + entry.getTimeBIP() / numX / 1000 + " (secs)");
        Rt.p(" AVG INUM = " + entry.getTimeInum() / numX / 1000 + " (secs) ");
        Rt.p(" TOTAL TIME = " + entry.getTotalTime() / 1000 + "(secs)");
    }
    
    
    /***************************************************
     * 
     * Draw elastic graph
     * 
     * 
     ***************************************************/
    public static void drawElastic() throws Exception
    {   
        int numX;
        elasticFile = new File(rawDataDir, wlName + "_" + ELASTIC_FILE);
        ElasticPaperEntry entry = readElasticResult(elasticFile);
        
        numX = entry.listReconfiguationCosts.size();
        double[] xtics = new double[numX];
        String[] xaxis = new String[numX];
        List<Point> points = new ArrayList<Point>();
        int round;
        for (int i = 0; i < numX; i++){
            xtics[i] = i;
            round = (int)(entry.listReconfiguationCosts.get(i) / Math.pow(10, 6));
            xaxis[i] = Integer.toString(round);
        }
        
        String[] competitors = new String[entry.listNumberReplicas.size()
                                          + 1];
        for (int i = 0; i < entry.listNumberReplicas.size(); i++)
            competitors[i] = "m = " + Integer.toString(
                     entry.listNumberReplicas.get(i));
        competitors[entry.listNumberReplicas.size()] = "CURRENT";
        
        int n;
        for (int i = 0; i < numX; i++) {
            
            for (int j = 0; j < competitors.length - 1; j++) {
                n = entry.listNumberReplicas.get(j);
                points.add(new Point(xtics[i], entry.mapReplicaCosts.get(n).get(i)));
            }
             
            points.add(new Point(xtics[i], entry.currentCost));
        }
       
        plotName = dbName + "_" + wlName + "elastic";
        xname = "Reconfiguration cost (x10^6)";
        yname = "Expected cost";
     
        drawLineGnuPlot(plotName, xname, yname, xaxis, xtics, 
                competitors, figsDir, points);
        entry.totalTime /= 1000;
        
        plots.add(new Plot("figs/" + plotName, 
                " Database = " + dbName + ", workload = " + wlName +
                " Elasticity, initial configuration includes the first 200 queries"
                + ". The running time to generate this graph = "
                + entry.totalTime + " secs ", 0.5));
               
    }
    
    /***************************************************
     * 
     * Draw failure graph
     * 
     * 
     ***************************************************/
    public static void drawFailure() throws Exception
    {   
        File file;
        File fileDesign;
        file = new File(rawDataDir, wlName + "_" + FAILURE_FILE);
        List<RobustPaperEntry> entries = readFailureImbalanceResult(file);
        
        // read from DIVGDESGIN
        List<Double> alphas = new ArrayList<Double>();
        for (RobustPaperEntry entry : entries)
            alphas.add(entry.failureFactor);
        fileDesign = new File(rawDataDir, wlName + "_" + DESIGN_DB2_FILE);
        mapDesign = readDivResult(fileDesign);
        List<Double> costDesignFailures = costUnderFailure(alphas, mapDesign);
        
        // ---------------------------------------------
        String[] competitors = {"UNIF", "DIVGDESIGN", "RITA"};
        int numX = entries.size();

        double[] xtics = new double[numX];
        String[] xaxis = new String[numX];
        List<Point> points = new ArrayList<Point>();
        RobustPaperEntry entry;
        double time = 0.0;
        double alpha;
        for (int i = 0; i < numX; i++) {
            entry = entries.get(i);
            alpha = entry.failureFactor;
            xaxis[i] = Double.toString(alpha);
            xtics[i] = i;
            
            points.add(new Point(i, entry.costUnif));
            points.add(new Point(i, costDesignFailures.get(i)));
            points.add(new Point(i, entry.costDivg));
            time += entry.timeDivg;
        }
        
        Rt.p(" time = " + time);
        plotName = dbName + "_" + wlName + "_failure";
        xname = "Prob. of failure";
        yname = "TotalCost";

        drawLineGnuPlot(plotName, xname, yname, xaxis, xtics, 
                competitors, figsDir, points);
        time /= 1000;
        
        plots.add(new Plot("figs/" + plotName, 
                " Database = " + dbName + ", workload = " + wlName +
                " Elasticity, initial configuration includes the first 200 queries"
                + ". The running time to generate this graph = "
                + time + " secs ", 0.5));
               
    }
    
    /***************************************************
     * 
     * Draw unseen queries
     * 
     * 
     ***************************************************/
    public static void drawUnseenQueries() throws Exception
    {   
        File file;
        file = new File(rawDataDir, wlName + "_" + ROUTING_UNSEEN_QUERY_DB2_FILE);
        List<RoutingUnseenQueriesEntry> entries = readUnseenQueriesResult(file);
        
        // ---------------------------------------------
        String[] competitors = {"UNIF", "RITA-routing", "RITA-bestcase"};
        int numX = entries.size();

        double[] xtics = new double[numX];
        String[] xaxis = new String[numX];
        List<Point> points = new ArrayList<Point>();
        RoutingUnseenQueriesEntry entry;
        for (int i = 0; i < numX; i++) {
            entry = entries.get(i);
            xaxis[i] = Integer.toString(i);
            xtics[i] = i;
            
            points.add(new Point(i, entry.costUNIF));
            points.add(new Point(i, entry.costSplitting));
            points.add(new Point(i, entry.costBestCase));
        }
        
        plotName = dbName + "_" + wlName + "_routing_unseen_queries";
        xname = "Different run";
        yname = "ExpTotalCost";

        drawLineGnuPlot(plotName, xname, yname, xaxis, xtics, 
                competitors, figsDir, points);
        
        plots.add(new Plot("figs/" + plotName, 
                " Database = " + dbName + ", workload = " + wlName +
                " ROUTE 10 UNSEEN QUERIES, the training workload consists" +
                " of 30 queries"
                , 0.5));
               
    }
    
    /***************************************************
     *
     * Draw imbalance and Failure
     * 
     * 
     ***************************************************/
    public static void drawImbalanceFailure() throws Exception
    {   
        /*
        File fileExact, fileGreedy, fileDesign;
        fileExact = new File(rawDataDir, wlName + "_" + FAILURE_IMBALANCE_EXACT_FILE);
        fileGreedy = new File(rawDataDir, wlName + "_" + FAILURE_IMBALANCE_GREEDY_FILE);
        
        // ---------------------------------------------
        String[] competitors = {"UNIF", "DIVGDESIGN", "DIVBIP"};
        int numX = entries.size();

        double[] xtics = new double[numX];
        String[] xaxis = new String[numX];
        List<Point> points = new ArrayList<Point>();
        RobustPaperEntry entry;
        double time = 0.0;
        double alpha;
        for (int i = 0; i < numX; i++) {
            entry = entries.get(i);
            alpha = entry.failureFactor;
            xaxis[i] = Double.toString(alpha);
            xtics[i] = i;

            Rt.p(" node factor = " + entry.nodeFactor);
            ratio = entry.getCostImprovement();
            timeExact += entry.timeDivg;
            if (ratio < 0)
                ratio = 0.0;            
            ratio = ratio * 100;
            points.add(new Point(i, ratio));
            
            points.add(new Point(i, entry.costUnif));
            points.add(new Point(i, costDesignFailures.get(i)));
            points.add(new Point(i, entry.costDivg));
            time += entry.timeDivg;
        }
        
        Rt.p(" time = " + time);
        plotName = dbName + "_" + wlName + "_failure";
        xname = "Prob. of failure";
        

        plotName = dbName + "_" + wlName + "_failure_imbalance";
        xname = "Imbalance factor";        
        yname = "TotalCost Improvement (%)";
        
        drawLineGnuPlot(plotName, xname, yname, xaxis, xtics, 
                competitors, figsDir, points);
        
        plots.add(new Plot("figs/" + plotName,  

                " IMBALANCE and FAILURE, space = 0.5x, n = 3, failure=  0.2"
                + "time EXACT = " + timeExact
                + " avg EXACT TIME = " + (timeExact / numX)
                + "time GREEDY = " + timeGreedy
                + " avg GREEDY TIME = " + (timeGreedy / numX),
                0.5));
        */
    }
    
    protected static List<Double> costUnderFailure(List<Double> alphas, Map<DivPaperEntry, Double> mapVal)
                throws Exception
    {
        Rt.p(" nReplicas = " + nReplicas
                + " space = " + B);
        DivPaperEntry entry = new DivPaperEntry(dbName, wlName, nReplicas, 
                convertBudgetToMB(B), null);
        double costWithoutFailure = 0.0;
        double costWithFailure = 0.0;
        DivConfiguration configuration = null;
        int n = -1;
        double totalCost;
        // traverse the map
        for (Map.Entry<DivPaperEntry, Double> ele : mapVal.entrySet()) {
            if (ele.getKey().equals(entry)) {
                costWithoutFailure = ele.getValue();
                configuration = ele.getKey().divConfiguration;
                costWithFailure = DivBIPFunctionalTest.getTotalDB2QueryCostDivConfFailure
                                (configuration);
                n = configuration.getNumberReplicas();
                costWithFailure =  costWithFailure / n; 
                Rt.p(" cost with failure = " + costWithFailure);
                break;
            }
        }
        Rt.p("After for loop");
        double scale;
        
        List<Double> results = new ArrayList<Double>();
        for (double alpha : alphas){
            scale = UtilConstraintBuilder.scaleDownFactor(alpha, n);
            totalCost = scale * costWithoutFailure * 
                UtilConstraintBuilder.computeCoefCostWithoutFailure(alpha, n);
            totalCost += scale * costWithFailure * 
                UtilConstraintBuilder.computeCoefCostWithFailure(alpha, n);
            
            results.add(totalCost);
        }
        
        return results;
    }
    
    /****************************************************
     *   Analyze divergent configuration
     * 
     * 
     *******************************/
    public static void analyzeDivConfiguration() 
                throws Exception
    {   
        DivPaperEntry entry = new DivPaperEntry(dbName, wlName, nReplicas, 
                convertBudgetToMB(B), null);
        // 1. Read the result from UNIF file
        unifFile = new File(rawDataDir, wlName + "_" + UNIF_DB2_FILE);
        mapUnif = readDivResult(unifFile);
        Rt.p(" read from file = " + unifFile.getAbsolutePath());
        
        // 2. Read the result from DIV file
        divFile = new File(rawDataDir, wlName + "_" + DIV_DB2_FILE);
        mapDiv = readDivResult(divFile);
        
        // 3. Read the result from Design file
        designFile = new File(rawDataDir, wlName + "_" + DESIGN_DB2_FILE);
        mapDesign = readDivResult(designFile);
        
        // traverse the map
        for (Map.Entry<DivPaperEntry, Double> ele : mapUnif.entrySet()) {
            if (ele.getKey().equals(entry)) {
                Rt.p("UNIF ------------");
                Rt.p(ele.getKey().divConfiguration.indexAtReplicaInfo());
                Rt.p(" number distinct indexes = " + 
                        ele.getKey().divConfiguration.countDistinctIndexes());
            }
        }
        
        // DIVGDESIGN
        for (Map.Entry<DivPaperEntry, Double> ele : mapDesign.entrySet()) {
            if (ele.getKey().equals(entry)) {
                Rt.p("DIVDESIGN ------------");
                Rt.p(ele.getKey().divConfiguration.indexAtReplicaInfo());
                Rt.p(" number distinct indexes = " + 
                        ele.getKey().divConfiguration.countDistinctIndexes());
            }
        }
        
        // DIV
        for (Map.Entry<DivPaperEntry, Double> ele : mapDiv.entrySet()) {
            if (ele.getKey().equals(entry)) {
                Rt.p("BIP ------------");
                Rt.p(ele.getKey().divConfiguration.indexAtReplicaInfo());
                Rt.p(" number distinct indexes = " + 
                        ele.getKey().divConfiguration.countDistinctIndexes());
            }
        }
    }

    /***************************************************
     * 
     * Draw imbalance
     * 
     * 
     ***************************************************/
    public static void drawImbalance() throws Exception
    {   
        File fileGreedy, fileIF;
        fileGreedy = new File(rawDataDir, wlName + "_" + IMBALANCE_GREEDY_FILE);
        fileIF = new File(rawDataDir, wlName + "_" + FAILURE_IMBALANCE_GREEDY_FILE);
        
        List<RobustPaperEntry> greedyEntries = readFailureImbalanceResult(fileGreedy);
        List<RobustPaperEntry> ifEntries = readFailureImbalanceResult(fileIF);

        
        Rt.p(" Number entries = " + greedyEntries.size());
        String[] competitors = {"UNIF", "DIVBIP, a = 0.1", "DIVBIP, a = 0"};

        int numX = greedyEntries.size();

        double[] xtics = new double[numX];
        String[] xaxis = new String[numX];
        List<Point> points = new ArrayList<Point>();
        RobustPaperEntry entry;
        double timeAlpha1 = 0.0;
        double timeAlpha0 = 0.0;
        
        for (int i = 0; i < numX; i++) {
            entry = ifEntries.get(i);
            timeAlpha1 += entry.timeDivg;
            xaxis[i] = Double.toString(entry.nodeFactor);
            xtics[i] = i;
            points.add(new Point(i, entry.costUnif));
            points.add(new Point(i, entry.costDivg));
            
            entry = greedyEntries.get(i);
            points.add(new Point(i, entry.costDivg));
            timeAlpha0 += entry.timeDivg;
        }
        
        plotName = dbName + "_" + wlName + "_imbalance";
        xname = "Load-imbalance factor";        
        yname = "TotalCost ";
        
        drawLineGnuPlot(plotName, xname, yname, xaxis, xtics, 
                competitors, figsDir, points);
        /*
        // 4. Read the result from Design file
        // Get the imbalance of the DIVGDESIGN
        designFile = new File(rawDataDir, wlName + "_" + DESIGN_DB2_FILE);
        mapDesign = readDivResult(designFile);
        DivPaperEntry entryP = new DivPaperEntry(dbName, wlName, nReplicas, 
                convertBudgetToMB(B), null);
        double designImbalance = -1;
        for (Map.Entry<DivPaperEntry, Double> ele : mapDesign.entrySet()) 
            if (ele.getKey().equals(entryP)) {
                designImbalance = computeLoadImbalance(ele.getKey().divConfiguration, workload);
                break;
            }
        // ------------------------------------------------------
        */    
        timeAlpha0 = timeAlpha0 / numX / 1000;
        timeAlpha1 = timeAlpha1 / numX / 1000;
        plots.add(new Plot("figs/" + plotName,  
                " IMBALANCE/FAILURE, space = 0.5x, n = 3"
                + " avg  TIME for alpha = 0 is " + timeAlpha0 + " (secs)"
                + " avg  TIME for alpha > 0 is " + timeAlpha1 + " (secs)"
                , 0.5));
    }

   
    /**
     * Write into a text file
     * @param fileName
     */
    protected static void readOnlineResultToFile(File file, List<Double> optCosts,
                                            List<Double> initialCosts) throws Exception
    {
        BufferedReader reader;        
        String line;
        
        reader = new BufferedReader(new FileReader(file));
        String[] tokens;
        while ((line = reader.readLine()) != null) {
            line = line.trim();
            tokens = line.split(",");
            assert(tokens.length == 2);
            optCosts.add(Double.parseDouble(tokens[0]));
            initialCosts.add(Double.parseDouble(tokens[1]));
        }
    }
    
    /**
     * Draw GnuPlot
     * 
     * 
     * @param plotName
     *      The caption of the plot     
     * @param xname
     *      The x-axis name
     * @param yname
     *      The y-axis name
     * @param xaxis
     *      The list of names at all the sticks 
     * @param xtics
     *      The positions of these sticks at x-axix
     * @param competitors
     *      The name of competitors methods
     * @param figsDir
     *      The location to store the generated figure
     * @param points
     *      The set of points to draw
     *      
     * @throws IOException
     *      If it cannot drive into files properly
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
    
    /**
     * Store the maps of divergent results into binary object file
     * 
     * @param maps
     *      The map that stores the result           
     * @param file
     *      The filename on which the data is written on
     *      
     * @throws Exception
     */
    protected static void serializeDivResult(Map<DivPaperEntry, Double> maps, File file) throws Exception 
    {    
        ObjectOutputStream write;
        Rt.p("Store in file = " + file.getName());
        try {
            FileOutputStream fileOut = new FileOutputStream(file, false);
            write = new ObjectOutputStream(fileOut);
            write.writeObject(maps);
            write.close();
            fileOut.close();
        } catch(IOException e) {
            throw new SQLException(e);
        }
    }
    
    @SuppressWarnings("unchecked")
    protected static Map<DivPaperEntry, Double> readDivResult(File file) throws Exception
    {
        ObjectInputStream in;
        Map<DivPaperEntry, Double> results = null;
        try {
            FileInputStream fileIn = new FileInputStream(file);
            in = new ObjectInputStream(fileIn);
            results = (Map<DivPaperEntry, Double>) in.readObject();

            in.close();
            fileIn.close();
        } catch(IOException e) {
            throw new SQLException(e);
        } catch (ClassNotFoundException e) {
            throw new SQLException(e);
        }
        
        return results;
    }
    
    /**
     * Store the maps of divergent results into binary object file
     * 
     * @param maps
     *      The map that stores the result           
     * @param file
     *      The filename on which the data is written on
     *      
     * @throws Exception
     */
    protected static void serializeDivResultDetail(Map<DivPaperEntryDetail, Double> 
                            maps, File file) throws Exception 
    {    
        ObjectOutputStream write;
        Rt.p("Store in file = " + file.getName());
        try {
            FileOutputStream fileOut = new FileOutputStream(file, false);
            write = new ObjectOutputStream(fileOut);
            write.writeObject(maps);
            write.close();
            fileOut.close();
        } catch(IOException e) {
            throw new SQLException(e);
        }
    }
    
    @SuppressWarnings("unchecked")
    protected static Map<DivPaperEntryDetail, Double> readDivResultDetail(File file) throws Exception
    {
        ObjectInputStream in;
        Map<DivPaperEntryDetail, Double> results = null;
        try {
            FileInputStream fileIn = new FileInputStream(file);
            in = new ObjectInputStream(fileIn);
            results = (Map<DivPaperEntryDetail, Double>) in.readObject();

            in.close();
            fileIn.close();
        } catch(IOException e) {
            throw new SQLException(e);
        } catch (ClassNotFoundException e) {
            throw new SQLException(e);
        }
        
        return results;
    }
    
    /**
     * Store the maps of divergent results into binary object file
     * 
     * @param maps
     *      The map that stores the result           
     * @param file
     *      The filename on which the data is written on
     *      
     * @throws Exception
     */
    protected static void serializeOnlineResult(OnlinePaperEntry onlineEntry, 
                                                File file) throws Exception 
    {    
        ObjectOutputStream write;
        
        try {
            FileOutputStream fileOut = new FileOutputStream(file, false);
            write = new ObjectOutputStream(fileOut);
            write.writeObject(onlineEntry);
            write.close();
            fileOut.close();
        } catch(IOException e) {
            throw new SQLException(e);
        }
    }
    
    
    protected static OnlinePaperEntry readOnlineResult(File file) throws Exception
    {
        ObjectInputStream in;
        OnlinePaperEntry results = null;
        try {
            FileInputStream fileIn = new FileInputStream(file);
            in = new ObjectInputStream(fileIn);
            results = (OnlinePaperEntry) in.readObject();

            in.close();
            fileIn.close();
        } catch(IOException e) {
            throw new SQLException(e);
        } catch (ClassNotFoundException e) {
            throw new SQLException(e);
        }
        
        return results;
    }
    
    
    /**
     * Store the maps of divergent results into binary object file
     * 
     * @param maps
     *      The map that stores the result           
     * @param file
     *      The filename on which the data is written on
     *      
     * @throws Exception
     */
    protected static void serializeElasticResult(ElasticPaperEntry elasticEntry, 
                                                File file) throws Exception 
    {    
        ObjectOutputStream write;
        
        try {
            FileOutputStream fileOut = new FileOutputStream(file, false);
            write = new ObjectOutputStream(fileOut);
            write.writeObject(elasticEntry);
            write.close();
            fileOut.close();
        } catch(IOException e) {
            throw new SQLException(e);
        }
    }
    
    
    protected static ElasticPaperEntry readElasticResult(File file) throws Exception
    {
        ObjectInputStream in;
        ElasticPaperEntry result = null;
        try {
            FileInputStream fileIn = new FileInputStream(file);
            in = new ObjectInputStream(fileIn);
            result = (ElasticPaperEntry) in.readObject();

            in.close();
            fileIn.close();
        } catch(IOException e) {
            throw new SQLException(e);
        } catch (ClassNotFoundException e) {
            throw new SQLException(e);
        }
        
        return result;
    }
    
    /**
     * Store the maps of divergent results into binary object file
     * 
     * @param maps
     *      The map that stores the result           
     * @param file
     *      The filename on which the data is written on
     *      
     * @throws Exception
     */
    protected static void serializeFailureImbalanceResult(List<RobustPaperEntry> entries, File file) throws Exception 
    {    
        ObjectOutputStream write;
        Rt.p("Store in file = " + file.getName());
        try {
            FileOutputStream fileOut = new FileOutputStream(file, false);
            write = new ObjectOutputStream(fileOut);
            write.writeObject(entries);
            write.close();
            fileOut.close();
        } catch(IOException e) {
            throw new SQLException(e);
        }
    }
    
    protected static List<RoutingUnseenQueriesEntry> readUnseenQueriesResult(File file) 
    throws Exception
    {
        ObjectInputStream in;
        List<RoutingUnseenQueriesEntry> results = null;
        try {
            FileInputStream fileIn = new FileInputStream(file);
            in = new ObjectInputStream(fileIn);
            results = (List<RoutingUnseenQueriesEntry>) in.readObject();

            in.close();
            fileIn.close();
        } catch(IOException e) {
            throw new SQLException(e);
        } catch (ClassNotFoundException e) {
            throw new SQLException(e);
        }

return results;
}
    
    protected static List<RobustPaperEntry> readFailureImbalanceResult(File file) 
            throws Exception
    {
        ObjectInputStream in;
        List<RobustPaperEntry> results = null;
        try {
            FileInputStream fileIn = new FileInputStream(file);
            in = new ObjectInputStream(fileIn);
            results = (List<RobustPaperEntry>) in.readObject();

            in.close();
            fileIn.close();
        } catch(IOException e) {
            throw new SQLException(e);
        } catch (ClassNotFoundException e) {
            throw new SQLException(e);
        }
        
        return results;
    }
    
    
            
    /**
     * Class of data points drawn in GnuPlot
     * 
     * @author Quoc Trung Tran
     *
     */
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
    
    /**
     * Convert budget into MB units
     * @param B
     * @return
     */
    protected static long convertBudgetToMB(double B)
    {
        return (long) B;
        //return (long) (B / Math.pow(2, 20));
    }
    


     /* This class stores the total cost of
     * each method (UNIF, DIVBIP, DIVGDESIN)
     * on a particular instance of the divergent 
     * problem: database name, workload name,
     * number of replicas, and space budget
     * 
     * @author Quoc Trung Tran
     *
     */
    public static class DivPaperEntry implements Serializable
    {
        private static final long serialVersionUID = 1L;
        public String dbName;
        public String wlName;
        public int n;
        public long B;
        private int fHashCode;
        public DivConfiguration divConfiguration;
        // TODO: include the running time
        
        public DivPaperEntry(String _db, String _wl, int _n, long _B,
                            DivConfiguration _divConfiguration)
        {
            dbName = _db;
            wlName = _wl;
            n = _n;
            B = _B;
            divConfiguration = _divConfiguration;
        }

        @Override
        public boolean equals(Object obj) 
        {   
            if (!(obj instanceof DivPaperEntry))
                return false;
            
            DivPaperEntry competitor = (DivPaperEntry) obj;    
            if ( !(this.dbName.equals(competitor.dbName)) ||   
                 !(this.wlName.equals(competitor.wlName)) ||
                 (this.n != competitor.n) ||  
                 (this.B != competitor.B))  
                return false;
            
            return true;
        }
        
        @Override
        public String toString()
        {
            StringBuilder sb = new StringBuilder();
            sb.append("db = " + dbName + "\n")
              .append("wl = " + wlName + "\n")
              .append("number of replicas = " + n + "\n")
              .append("space budget = " + (B / Math.pow(2, 20)) + "\n");
            
            return sb.toString();
        }
        
        @Override
        public int hashCode() 
        {
            if (fHashCode == 0) {
                int result = HashCodeUtil.SEED;
                result = HashCodeUtil.hash(result, this.dbName.hashCode());
                result = HashCodeUtil.hash(result, this.wlName.hashCode());
                result = HashCodeUtil.hash(result, this.n);
                result = HashCodeUtil.hash(result, this.B);
                fHashCode = result;
            }
            
            return fHashCode;
        }
    }
    
    public static class DivPaperEntryDetail extends DivPaperEntry
                    implements Serializable
    {
        /**
         * Constructor
         * @param db
         *      Database name
         * @param wl
         *      Workload name
         * @param n
         *      Number of replicas
         * @param b
         *      Space budget
         * @param divConfiguration
         *      Divergent configuration
         */
        public DivPaperEntryDetail(String db, String wl, int n, long b,
                DivConfiguration divConfiguration) 
        {
            super(db, wl, n, b, divConfiguration);
        }

        private static final long serialVersionUID = 1L;
        public double queryCost;
        public double updateCost;
        public double loadImbalance;
    }
    
    /**
     * Online result
     * @author Quoc Trung Tran
     *
     */
    public static class OnlinePaperEntry implements Serializable
    {
        private static final long serialVersionUID = 1L;
        private List<Double> initialCosts;
        private List<Double> optCosts;
        private List<Integer> stmtReconfiguration;
        
        private double timeBIP;
        private double timeINUM;
        private double totalTime;
        
        int windowDuration;
        
        public OnlinePaperEntry()
        {
            initialCosts = new ArrayList<Double>();
            optCosts = new ArrayList<Double>();
            stmtReconfiguration = new ArrayList<Integer>();
        }
        
        public void addCosts(double initial, double opt)
        {
            this.initialCosts.add(initial);
            this.optCosts.add(opt);
        }
        
        public void addReconfiguration(int id)
        {
            this.stmtReconfiguration.add(id);
        }
        
        public void setTimes(double bip, double inum, double total)
        {
            this.timeBIP = bip;
            this.timeINUM = inum;
            this.totalTime = total;
        }
        
        public void setWindowDuration(int w)
        {
            this.windowDuration = w;
        }
        
        public double getTimeBIP()
        {
            return timeBIP;
        }

        public int getWindowDuration()
        {
            return this.windowDuration;
        }
        
        public double getTimeInum()
        {
            return timeINUM;
        }
        
        public double getTotalTime()
        {
            return totalTime;
        }
        
        public List<Double> getListInitial()
        {
            return this.initialCosts;
        }
        
        public List<Double> getListOpt()
        {
            return this.optCosts;
        }
        
        public List<Integer> getReconfigurationStmts()
        {
            return this.stmtReconfiguration;
        }
    }
    
    /**
     * Store the results of evaluating failures
     * 
     * @author Quoc Trung Tran
     *
     */
    public static class RobustPaperEntry implements Serializable
    {
        private static final long serialVersionUID = 1L;
   
        public double failureFactor;
        public double nodeFactor; // usually 0.0
        public double costDivg;
        public double costUnif;
        public double timeDivg;
        
        public DivConfiguration divConf;
        
        public RobustPaperEntry(double failure, double node,
                                double divg, double unif,
                                double time)
        {
            this.failureFactor = failure;
            this.nodeFactor = node;
            this.costDivg = divg;
            this.costUnif = unif;
            this.timeDivg = time;
        }
        
        public double getCostImprovement()
        {
            return 1 - (costDivg/ costUnif);
        }       
    }
    
    /**
     * Store the results of online features
     * 
     * @author Quoc Trung Tran
     *
     */
    public static class ElasticPaperEntry implements Serializable
    {
        private static final long serialVersionUID = 1L;
        public double totalTime;
        public double currentCost;
        public List<Integer> listNumberReplicas;
        public List<Double> listReconfiguationCosts;
        public Map<Integer, List<Double>> mapReplicaCosts;    
        
        public ElasticPaperEntry()
        {
            totalTime = 0.0;
            listNumberReplicas = new ArrayList<Integer>();
            listReconfiguationCosts = new ArrayList<Double>();
            mapReplicaCosts = new HashMap<Integer, List<Double>>();
        }
    }
    
    /**
     * Store the results of online features
     * 
     * @author Quoc Trung Tran
     *
     */
    public static class RoutingUnseenQueriesEntry implements Serializable
    {
        private static final long serialVersionUID = 1L;
        public DivConfiguration divTraining;
        public DivConfiguration divTesting;
        public double costBestCase;
        public double costSplitting;
        public double costUNIF;
        
        public RoutingUnseenQueriesEntry()
        {
         
        }
    }
    
    /**
     * Details of workload cost
     * @author tqtrung
     *
     */
    public static class WorkloadCostDetail
    {
        public double queryCost;
        public double updateCost;
        public double totalCost;
        
        public WorkloadCostDetail(double q, double u, double t)
        {
            queryCost = q;
            updateCost = u;
            totalCost = t;
        }
        
        public WorkloadCostDetail()
        {
            
        }
        
        @Override
        public String toString()
        {
            StringBuilder sb = new StringBuilder();
            sb.append("query cost = " + queryCost + "\n")
              .append("update cost = " + updateCost + "\n")
              .append("total cost = " + totalCost + "\n");
            
            return sb.toString();
        }
    }
}
