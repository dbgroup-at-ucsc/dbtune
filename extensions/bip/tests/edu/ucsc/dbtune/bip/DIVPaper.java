package edu.ucsc.dbtune.bip;

import static edu.ucsc.dbtune.DatabaseSystem.newDatabaseSystem;
import static edu.ucsc.dbtune.bip.CandidateGeneratorFunctionalTest.readCandidateIndexes;
import static edu.ucsc.dbtune.util.TestUtils.workload;
import static edu.ucsc.dbtune.workload.SQLCategory.DELETE;
import static edu.ucsc.dbtune.workload.SQLCategory.INSERT;
import static edu.ucsc.dbtune.workload.SQLCategory.SELECT;
import static edu.ucsc.dbtune.workload.SQLCategory.UPDATE;

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

import edu.ucsc.dbtune.advisor.db2.DB2Advisor;
import edu.ucsc.dbtune.bip.div.DivBIP;
import edu.ucsc.dbtune.bip.div.DivConfiguration;
import edu.ucsc.dbtune.bip.util.LatexGenerator;
import edu.ucsc.dbtune.bip.util.LatexGenerator.Plot;
import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.optimizer.InumOptimizer;
import edu.ucsc.dbtune.util.Environment;
import edu.ucsc.dbtune.util.GnuPlotLine;
import edu.ucsc.dbtune.util.HashCodeUtil;
import edu.ucsc.dbtune.util.Rt;
import edu.ucsc.dbtune.workload.SQLStatement;


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
    
    protected static String dbNames[] = {"test"}; 
    protected static String wlNames[] = {"tpcds"};
    
    protected static Map<DivPaperEntry, Double> entries;
    
    protected static final String DIV_DB2_FILE = "div_db2.bin";
    protected static final String UNIF_DB2_FILE = "unif_db2.bin";
    protected static final String DESIGN_DB2_FILE = "design_db2.bin";
    protected static final String DESIGN_COPHY_FILE = "design_cophy.bin";
    protected static final String UNIF_COPHY_FILE = "unif_cophy.bin";
    
    protected static final String ONLINE_FILE = "online.txt";
    
    protected static File unifFile;
    protected static File divFile;
    protected static File designFile;
    protected static File unifCoPhyFile;
    protected static File designCoPhyFile;
    protected static File onlineFile;
    
    protected static Map<DivPaperEntry, Double> mapUnif;
    protected static Map<DivPaperEntry, Double> mapDiv;
    protected static Map<DivPaperEntry, Double> mapDesign;    
    protected static Map<DivPaperEntry, Double> mapDesignCoPhy;
    protected static Map<DivPaperEntry, Double> mapUnifCoPhy;
    
    protected static boolean isEquivalent = true;
    protected static boolean isOnline = false;
    protected static boolean isLatex = false;
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
        setParameters();
        
        // 2. draw graphs
        if (isEquivalent)
            for (int i = 0; i < dbNames.length; i++) {
                drawGraphDIVEquivBIP(dbNames[i], wlNames[i], true);
                drawGraphDIVEquivBIP(dbNames[i], wlNames[i], false);
            }
        
        if (isOnline)
            drawOnline();
        
        isLatex = isOnline || isEquivalent;
        if (isLatex)
            LatexGenerator.generateLatex(latexFile, outputDir, plots);
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
    public static void drawGraphDIVEquivBIP(String dbName, String wlName, boolean drawRatio) 
                throws Exception
    {   
        DivPaperEntry entry;
        
        // 1. Read the result from UNIF file
        unifFile = new File(rawDataDir, UNIF_DB2_FILE);
        mapUnif = readDivResult(unifFile);
        
        // 2. Read the result from DIV file
        divFile = new File(rawDataDir, DIV_DB2_FILE);
        mapDiv = readDivResult(divFile);
        Rt.p(" map div: " + mapDiv);
        
        // 3. Read the result from Design file
        designFile = new File(rawDataDir, DESIGN_DB2_FILE);
        mapDesign = readDivResult(designFile);
        
        String[] competitors;
        
        // 3. draw graphs
        if (drawRatio)
            competitors = new String[] {"1 - DIV-BIP/UNIF", " 1 - DIVGDESIGN/UNIF"};
        else 
            competitors = new String[] {"DIV-BIP", "DIVGDESIGN", "UNIF"};
        int numX;
        double ratio; 
        long budget;
        
        // varying number of replicas
        for (double B : listBudgets) {
            budget = convertBudgetToMB (B);
            int n;
            numX = listNumberReplicas.size();
            double[] xtics = new double[numX];
            String[] xaxis = new String[numX];
            List<Point> points = new ArrayList<Point>();
            
            for (int i = 0; i < numX; i++) {
                n = listNumberReplicas.get(i);
                xtics[i] = i;
                xaxis[i] = Integer.toString(n);
                entry = new DivPaperEntry(dbName, wlName, n, budget);
                if (drawRatio)
                    addPointRatioDIVEquivBIP(xtics[i], entry, points);
                else 
                    addPointDIVEquivBIP(xtics[i], entry, points);
            }
            
            ratio = (double) B / Math.pow(2, 30) / 10;
            plotName = dbName + "_" + wlName + "_space_" + Double.toString(ratio) + "x";
            if (drawRatio)
                plotName += "_ratio";
            else
                plotName += "_absolute";
            xname = "# replicas";
            if (drawRatio)
                yname = "TotalCost improvement (%)";
            else 
                yname = "TotalCost";
         
            drawLineGnuPlot(plotName, xname, yname, xaxis, xtics, 
                    competitors, figsDir, points);
            
            plots.add(new Plot("figs/" + plotName, 
                    " Database = " + dbName + " workload = " + wlName +
                    " Varying \\# replicas, B = " + Double.toString(ratio) + "x", 0.5));
        }
        
        
        // varying space budget
        for (int n : listNumberReplicas) {
            double B;
            numX = listBudgets.size();
            double[] xtics = new double[numX];
            String[] xaxis = new String[numX];
            List<Point> points = new ArrayList<Point>();
            
            for (int i = 0; i < numX; i++) {
                B = listBudgets.get(i);
                budget = convertBudgetToMB (B);
                xtics[i] = i;
                ratio = (double) B / Math.pow(2, 30) / 10;
                xaxis[i] = Double.toString(ratio) + "x";
                entry = new DivPaperEntry(dbName, wlName, n, budget);
                if (drawRatio)
                    addPointRatioDIVEquivBIP(xtics[i], entry, points);
                else 
                    addPointDIVEquivBIP(xtics[i], entry, points);
            }
            
            
            plotName = dbName + "_" + wlName + "_number_replica" + Integer.toString(n);
            if (drawRatio)
                plotName += "_ratio";
            else
                plotName += "_absolute";
            
            xname = "Space budget";
            if (drawRatio)
                yname = "TotalCost improvement (%)";
            else 
                yname = "TotalCost";
            
            drawLineGnuPlot(plotName, xname, yname, xaxis, xtics, 
                    competitors, figsDir, points);
            
            plots.add(new Plot("figs/" + plotName, 
                    " Database = " + dbName + " workload = " + wlName +
                    " Varying space budgets, n = " + Integer.toString(n), 0.5));
        }
    }
    
    
    /**
     * Add corresponding points into the list of points, which are 
     * displayed in GnuPlot
     * @param entry
     * @param points
     */
    protected static void addPointRatioDIVEquivBIP(double xcoordinate, DivPaperEntry entry, List<Point> points)
    {
        double costDiv, costUnif, costDesign;
        double ratioDesign, ratioDiv;
        
        costDiv = mapDiv.get(entry);
        costUnif = mapUnif.get(entry);
        costDesign = mapDesign.get(entry);
        
        Rt.p(" entry: " + entry);
        Rt.p(" cost UNIF = " + (costUnif / Math.pow(10, 6)));
        Rt.p(" cost DIV = " + (costDiv / Math.pow(10, 6)));
        Rt.p(" cost DESIGN = " + (costDesign / Math.pow(10, 6)));
        
        ratioDiv = 1 - (double) costDiv / costUnif;
        ratioDesign = 1 - (double) costDesign / costUnif;
        ratioDiv = ratioDiv * 100;
        ratioDesign = ratioDesign * 100;
        if (ratioDiv < 0.0) {
            Rt.p(" watch out, entry = " + entry
                    + ", ratio DIV = " + ratioDiv);
            ratioDiv = 0.05;  
        }
        
        if (ratioDesign < 0.0){
            Rt.p("watch out, entry = " + entry 
                    + ", ratioDesign = " + ratioDesign);
            ratioDesign = 0.05;
        }
        
        points.add(new Point(xcoordinate, ratioDiv));
        points.add(new Point(xcoordinate, ratioDesign));
    }
    
    /**
     * Add corresponding points into the list of points, which are 
     * displayed in GnuPlot
     * @param entry
     * @param points
     */
    protected static void addPointDIVEquivBIP(double xcoordinate, DivPaperEntry entry, List<Point> points)
    {
        double costDiv, costUnif, costDesign;
        Rt.p(" entry = " + entry); 
        costDiv = mapDiv.get(entry);
        costUnif = mapUnif.get(entry);
        costDesign = mapDesign.get(entry);
        
        
        points.add(new Point(xcoordinate, costDiv));
        points.add(new Point(xcoordinate, costDesign));
        points.add(new Point(xcoordinate, costUnif));
    }
    
    
    
    
    /*
     * Draw online graph
     */
    /**
     * Read the data from files and draw the graphs
     */
    public static void drawOnline() throws Exception
    {   
        onlineFile = new File(rawDataDir, ONLINE_FILE);
        OnlinePaperEntry entry = readOnlineResult(onlineFile);
        
        String[] competitors = {" 1 - OPT / INITIAL"};
        
        int numX = entry.getListInitial().size();
        // the last two for the running time
        double ratio;
        double[] xtics = new double[numX];
        String[] xaxis = new String[numX];
        List<Point> points = new ArrayList<Point>();
        Map<Integer, Integer> ticks = new HashMap<Integer, Integer>();
        for (int id : entry.getReconfigurationStmts())
            ticks.put(id, id);
            
        for (int i = 0; i < numX; i++) {
            ratio = 1 - entry.getListOpt().get(i) / entry.getListInitial().get(i);
            ratio = ratio * 100;
            
            if (i == 0 || ticks.containsKey(i))
                xaxis[i] = Integer.toString(i);
            else
                xaxis[i] = "null"; // NOT mark this
            xtics[i] = i;
            points.add(new Point(i, ratio));
        }
        
        plotName = "online";
        xname = "#query ID";
        yname = "TotalCost improvement (%)";
        
        drawLineGnuPlot(plotName, xname, yname, xaxis, xtics, 
                competitors, figsDir, points);
        
        plots.add(new Plot("figs/" + plotName,  
                " ONLINE, space = 0.5x, n = 3", 0.5));
        
        Rt.p("reconfiguration: " + entry.getReconfigurationStmts());
        Rt.p(" AVG BIP = " + entry.getTimeBIP() / numX / 1000 + " (secs)");
        Rt.p(" AVG INUM = " + entry.getTimeInum() / numX / 1000 + " (secs) ");
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
        db2Advis = new DB2Advisor(db);
        candidates = readCandidateIndexes(db2Advis);
        
        long totalSize = 0;
        for (Index i : candidates)
            totalSize += i.getBytes();
        
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
        
        div = new DivBIP();
        
        Rt.p("DIVpaper: # statements in the workload =  " + workload.size()
                + " # candidates in the workload = " + candidates.size()
                + " Total size = " + (totalSize / Math.pow(2, 20))
                + " workload folder = " + folder);
    }
    
    /**
     * Set common parameter (e.g., workload, number of replicas, etc. )
     * 
     * @throws Exception
     */
    public static void setParameters() throws Exception
    {   
        // Debugging parameters
        isExportToFile = false;
        isTestCost = false;
        isShowRecommendation = true;        
        isGetAverage = false;
        isDB2Cost = false;
        isAllImbalanceConstraint = false;
        
        // space budget -- 10GB
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
    
    public static class OnlinePaperEntry implements Serializable
    {
        private static final long serialVersionUID = 1L;
        private List<Double> initialCosts;
        private List<Double> optCosts;
        private List<Integer> stmtReconfiguration;
        
        private double timeBIP;
        private double timeINUM;
        
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
        
        public void setTimes(double bip, double inum)
        {
            this.timeBIP = bip;
            this.timeINUM = inum;
        }
        
        public double getTimeBIP()
        {
            return timeBIP;
        }
        
        public double getTimeInum()
        {
            return timeINUM;
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
     * This class stores the total cost of 
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
        private String dbName;
        private String wlName;
        private int n;
        private long B;
        private int fHashCode;
        
        public DivPaperEntry(String _db, String _wl, int _n, long _B)
        {
            dbName = _db;
            wlName = _wl;
            n = _n;
            B = _B;
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
    
    
    /**
     * Read the data from files and draw the graphs
     */
    public static void drawAllGraphDIVEquivBIP(String dbName, String wlName, boolean drawRatio) 
            throws Exception
    {   
        DivPaperEntry entry;
        
        // 1. Read the result from UNIF file
        unifFile = new File(rawDataDir, UNIF_DB2_FILE);
        mapUnif = readDivResult(unifFile);
        
        // 2. Read the result from DIV file
        divFile = new File(rawDataDir, DIV_DB2_FILE);
        mapDiv = readDivResult(divFile);
        
        // 3. Read the result from UNIF-COPHY file
        unifCoPhyFile = new File(rawDataDir, UNIF_COPHY_FILE);
        mapUnifCoPhy = readDivResult(unifCoPhyFile);
        
        String[] competitors;
        
        // 3. draw graphs
        if (drawRatio)
            competitors = new String[] {"1 - DIV-BIP/UNIF-DB2", "1 - DIV-BIP/UNIF-COPHY"}; 
        else 
            competitors = new String[] {"DIV-BIP", "UNIF-COPHY", "UNIF-DB2"};
        int numX;
        double ratio; 
        long budget;
        
        // varying space budget
        for (int n : listNumberReplicas) {
            double B;
            numX = listBudgets.size();
            double[] xtics = new double[numX];
            String[] xaxis = new String[numX];
            List<Point> points = new ArrayList<Point>();
            
            for (int i = 0; i < numX; i++) {
                B = listBudgets.get(i);
                budget = convertBudgetToMB (B);
                xtics[i] = i;
                ratio = (double) B / Math.pow(2, 30) / 10;
                xaxis[i] = Double.toString(ratio) + "x";
                entry = new DivPaperEntry(dbName, wlName, n, budget);
                if (drawRatio)
                    addAllPointRatioDIVEquivBIP(xtics[i], entry, points);
                else 
                    addAllPointDIVEquivBIP(xtics[i], entry, points);
            }
            
            plotName = dbName + "_" + wlName + "_full_number_replica" + Integer.toString(n);
            if (drawRatio)
                plotName += "_ratio";
            else
                plotName += "_absolute";
            xname = "Space budget";
            if (drawRatio)
                yname = "TotalCost improvement (%)";
            else 
                yname = "TotalCost";
            
            drawLineGnuPlot(plotName, xname, yname, xaxis, xtics, 
                    competitors, figsDir, points);
            
            plots.add(new Plot("figs/" + plotName, 
                    " Database = " + dbName + " workload = " + wlName +
                    " Varying space budgets, n = " + Integer.toString(n), 0.5));
        }
    }
    
    
    /**
     * Add corresponding points into the list of points, which are 
     * displayed in GnuPlot
     * @param entry
     * @param points
     */
    protected static void addAllPointRatioDIVEquivBIP(double xcoordinate, DivPaperEntry entry, List<Point> points)
    {
        double costDiv, costUnif, costUnifCoPhy;
        double ratioCoPhy, ratioDiv;
        
        costDiv = mapDiv.get(entry);
        costUnif = mapUnif.get(entry);
        costUnifCoPhy = mapUnifCoPhy.get(entry);
        
        Rt.p(" entry: " + entry);
        Rt.p(" cost UNIF = " + (costUnif / Math.pow(10, 6)));
        Rt.p(" cost DIV = " + (costDiv / Math.pow(10, 6)));
        Rt.p(" cost UNIF-COPHY = " + (costUnifCoPhy / Math.pow(10, 6)));
        
        ratioDiv = 1 - (double) costDiv / costUnif;
        ratioCoPhy = 1 - (double) costDiv / costUnifCoPhy;
        ratioDiv = ratioDiv * 100;
        ratioCoPhy = ratioCoPhy * 100;
        if (ratioDiv < 0.0) {
            Rt.p(" watch out, entry = " + entry
                    + ", ratio DIV = " + ratioDiv);
            ratioDiv = 0.05;  
        }
        
        if (ratioCoPhy < 0.0){
            Rt.p("watch out, entry = " + entry 
                    + ", ratioCoPhy = " + ratioCoPhy);
            ratioCoPhy = 0.05;
        }
        
        points.add(new Point(xcoordinate, ratioDiv));
        points.add(new Point(xcoordinate, ratioCoPhy));
    }
    
    /**
     * Add corresponding points into the list of points, which are 
     * displayed in GnuPlot
     * @param entry
     * @param points
     */
    protected static void addAllPointDIVEquivBIP(double xcoordinate, DivPaperEntry entry, List<Point> points)
    {
        double costDiv, costUnif, costUnifCoPhy;
        
        costDiv = mapDiv.get(entry);
        costUnifCoPhy = mapUnifCoPhy.get(entry);
        costUnif = mapUnif.get(entry);
        
        points.add(new Point(xcoordinate, costDiv));
        points.add(new Point(xcoordinate, costUnifCoPhy));
        points.add(new Point(xcoordinate, costUnif));
    }
}
