package edu.ucsc.dbtune.bip;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import edu.ucsc.dbtune.bip.core.QueryPlanDesc;
import edu.ucsc.dbtune.bip.div.DivBIP;
import edu.ucsc.dbtune.bip.div.DivConfiguration;
import edu.ucsc.dbtune.bip.div.OnlineDivBIP;
import edu.ucsc.dbtune.bip.util.LatexGenerator;
import edu.ucsc.dbtune.bip.util.LogListener;
import edu.ucsc.dbtune.bip.util.LatexGenerator.Plot;
import edu.ucsc.dbtune.optimizer.InumOptimizer;
import edu.ucsc.dbtune.optimizer.Optimizer;
import edu.ucsc.dbtune.util.Rt;
import edu.ucsc.dbtune.workload.SQLStatement;
import edu.ucsc.dbtune.workload.Workload;

import static edu.ucsc.dbtune.bip.util.LogListener.EVENT_SOLVING_BIP;

public class OnlineDivBIPTest extends DIVPaper 
{   
    // number of statements to generate graphs 
    protected static int numStatementsOnline;
    // window duration
    protected static int windowDuration;
    
    protected static Workload wlInitial;
    protected static Workload wlOnline;
    
    protected static OnlineDivBIP onlineDiv;
    protected static OnlinePaperEntry onlineEntry;
    
    protected static boolean isOnline = true;
    protected static boolean isElasticity = false;
    
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
       
       // get database instances, candidate indexes
       getEnvironmentParameters();
       setParameters();
       
       // set control knobs
       controlKnobs();
       
       // prepare workload
       prepareWorkload();
       
       if (isOnline) {
           getInitialConfiguration(0, -1);
           initializeOnlineObject();
           runOnline();
       }
       
       if (isElasticity) {
           initializeOnlineObject();
           runElasticity();
           // move this functionality to DivPaper later
           LatexGenerator.generateLatex(latexFile, outputDir, plots);
       }
   }
   
    
   /**
    * Set the control knobs
    */
   protected static void controlKnobs()
   {
       windowDuration = en.getWindowDuration();
       numStatementsOnline = en.getNumberOfRunningQueries();
       Rt.p(" window duration = " + windowDuration);
       Rt.p(" # running queries = " + numStatementsOnline);
   }
   
   /**
    * split the input workload into the one used to find
    * initial configuration and for online workload
    */
   protected static void prepareWorkload()
   {   
       wlOnline = workload;
   }
   
   /**
    * Initial online object to run the expt.
    */
   protected static void initializeOnlineObject() throws Exception
   {
       onlineDiv = new OnlineDivBIP();
       onlineDiv.setCandidateIndexes(candidates);
       onlineDiv.setCommunicatingInumOnTheFly(false);
       onlineDiv.setDivBIP(div);
       onlineDiv.setInitialConfiguration(initialConf);
       onlineDiv.setOptimizer(io);
       onlineDiv.setWindowDuration(windowDuration);
       onlineDiv.setWorkload(wlOnline);
       onlineDiv.populateQueryPlanDescriptions();
   }
   
   /**
    * Run online experiments
    * @throws Exception
    */
   protected static void runOnline() throws Exception
   {   
       onlineFile = new File(rawDataDir, wlName + "_" + ONLINE_FILE);
       onlineFile.delete();
       onlineFile = new File(rawDataDir, wlName + "_" + ONLINE_FILE);
       onlineFile.createNewFile();
    
       onlineEntry = new OnlinePaperEntry();
       
       // populate INUM space
       LogListener logger = LogListener.getInstance();
       onlineDiv.setLogListenter(logger);
       
       double timeBIP, timeInum;
       timeBIP = 0.0;
       timeInum = 0.0;
       Rt.p("Number of statements = " + numStatementsOnline);
       onlineEntry.setWindowDuration(windowDuration);
       
       for (int i = 0; i < numStatementsOnline; i++) {
           logger.reset();
           onlineDiv.next();
           
           timeBIP += logger.getRunningTime(EVENT_SOLVING_BIP);
           timeInum += logger.getRunningTime(EVENT_SOLVING_BIP);
           
           /**
            * Compare with initial configuration
            * Might need to revisit later
            */
           
           onlineEntry.addCosts(onlineDiv.getTotalCostInitialConfiguration(),
                   onlineDiv.getTotalCost());
           if (onlineDiv.isNeedToReconfiguration()) {
               onlineEntry.addReconfiguration(i);
               Rt.p(" RECONFIGURATION AT : " + i);
           }
           
           //onlineEntry.addCosts(-1, onlineDiv.getTotalCost());
           
           if (i % 20 == 0) {
               Rt.p(" AFTER process statement: " + i);
               onlineEntry.setTimes(timeBIP, timeInum);
               Rt.p(" store in file = " + onlineFile.getName());
               serializeOnlineResult(onlineEntry, onlineFile);
           }
       }
       
       onlineEntry.setTimes(timeBIP, timeInum);
       Rt.p(" store in file = " + onlineFile.getName());
       serializeOnlineResult(onlineEntry, onlineFile);
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
       int startInitial = 190;
       int endInitial = 200;
       int endInvestigation = 280;
       
       // get initial configuration
       // empty configuration
       getInitialConfiguration(startInitial, endInitial);
       
       // set up the new workload
       List<SQLStatement> sqls = new ArrayList<SQLStatement>();
       for (int i = endInitial; i <= endInvestigation; i++)
           sqls.add(wlOnline.get(i));
       workload = new Workload(sqls);
       
       List<QueryPlanDesc> descs = onlineDiv.getQueryPlanDescs(endInitial, endInvestigation);
       
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
     * Get initial configuration for the system
     * @throws Exception
     */
    protected static void getInitialConfiguration(int startID, int endID) 
                    throws Exception
    {
        // empty INITIAL configuration
        if (endID == -1){
            Rt.p(" EMPTY INTIAL CONFIGURATION");
            div = new DivBIP();
            LogListener logger = LogListener.getInstance();
            // Derive corresponding parameters
            loadfactor = (int) Math.ceil( (double) nReplicas / 2);
            
            Optimizer io = db.getOptimizer();
            div.setCandidateIndexes(candidates);
            div.setWorkload(workload); 
            div.setOptimizer((InumOptimizer) io);
            div.setNumberReplicas(nReplicas);
            div.setLoadBalanceFactor(loadfactor);
            div.setSpaceBudget(B);
            div.setLogListenter(logger);
            
            initialConf = new DivConfiguration(nReplicas, loadfactor);
            return;
        }
            
        // Extract from startID to end ID of wlOnline
        List<SQLStatement> sqls = new ArrayList<SQLStatement>();
        for (int i = startID; i<= endID; i++)
            sqls.add(wlOnline.get(i));
        
        workload = new Workload(sqls);
       
        List<QueryPlanDesc> descs = onlineDiv.getQueryPlanDescs(startID, endID);
        Rt.p("Initial configuration: " + workload.size());
        DivBIPFunctionalTest.testDiv(nReplicas, B, descs);
        initialConf = divConf;
    }
}
