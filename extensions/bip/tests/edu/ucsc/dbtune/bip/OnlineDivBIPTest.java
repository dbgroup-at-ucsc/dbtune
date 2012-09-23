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
    // number of statements to create the initial configuration
    protected static int numStatementsInitialConfiguration;
    // number of statements to generate graphs 
    protected static int numStatementsOnline;
    // window duration
    protected static int windowDuration;
    
    protected static Workload wlInitial;
    protected static Workload wlOnline;
    
    protected static OnlineDivBIP onlineDiv;
    protected static OnlinePaperEntry onlineEntry;
    
    protected static boolean isOnline = false;
    protected static boolean isElasticity = true;
    
    /**
    *
    * Generate paper results
    */
   @Test
   public void main() throws Exception
   {
       dbNames = new String[] {"test"}; 
       wlNames = new String[] {"tpcds-online"};
       
       // 1. initialize file locations & necessary 
       // data structures
       initialize();
       
       // get database instances, candidate indexes
       getEnvironmentParameters(dbNames[0], wlNames[0]);
       
       // get parameters
       setParameters();
       
       // set control knobs
       controlKnobs();
       
       // prepare workload
       prepareWorkload();
       
       // default parameter
       nReplicas = 3;
       B = 2.5 * Math.pow(2, 30);
       
       // set up online workload
       initializeOnlineObject();
       
       if (isOnline)
           runOnline();
       
       if (isElasticity) {
           runElasticity(dbNames[0], wlNames[0]);
           // move this functionality to DivPaper later
           LatexGenerator.generateLatex(latexFile, outputDir, plots);
       }
   }
   
    
   /**
    * Set the control knobs
    */
   protected static void controlKnobs()
   {
       numStatementsInitialConfiguration = 10;
       numStatementsOnline = workload.size() - numStatementsInitialConfiguration;
       windowDuration = 100;
   }
   
   /**
    * split the input workload into the one used to find
    * initial configuration and for online workload
    */
   protected static void prepareWorkload()
   {
       List<SQLStatement> sqlInitials = new ArrayList<SQLStatement>();
       List<SQLStatement> sqlOnlines = new ArrayList<SQLStatement>();
       
       for (int i = 0; i < workload.size(); i++)
           if (i < numStatementsInitialConfiguration)
               sqlInitials.add(workload.get(i));
           else
               sqlOnlines.add(workload.get(i));
       
       wlInitial = new Workload(sqlInitials);
       wlOnline = new Workload(sqlOnlines);
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
       getInitialConfiguration(0, -1);
       
       onlineFile = new File(rawDataDir, ONLINE_FILE);
       onlineFile.delete();
       onlineFile = new File(rawDataDir, ONLINE_FILE);
       onlineFile.createNewFile();
    
       onlineEntry = new OnlinePaperEntry();
       
       // populate INUM space
       LogListener logger = LogListener.getInstance();
       onlineDiv.setLogListenter(logger);
       
       double timeBIP, timeInum;
       timeBIP = 0.0;
       timeInum = 0.0;
       
       for (int i = 0; i < numStatementsOnline; i++) {
           logger.reset();
           onlineDiv.next();
           
           timeBIP += logger.getRunningTime(EVENT_SOLVING_BIP);
           timeInum += logger.getRunningTime(EVENT_SOLVING_BIP);
           
           if (i % 20 == 0)
               Rt.p(" process statement: " + i);
           
           onlineEntry.addCosts(onlineDiv.getTotalCostInitialConfiguration(),
                   onlineDiv.getTotalCost());
           if (onlineDiv.isNeedToReconfiguration()) {
               onlineEntry.addReconfiguration(i);
               Rt.p(" RECONFIGURATION AT : " + i);
           }
           
           if (i >= 150)
               break;
       }
       
       onlineEntry.setTimes(timeBIP, timeInum);
       serializeOnlineResult(onlineEntry, onlineFile);
   }
   
   /**
    * Set up elasticity experiments
    * @throws Excpetion
    */
   protected static void runElasticity(String dbName, String wlName) throws Exception
   {
       // these can come from the list of sticks
       // from the online expt.
       // for now: they are hard-coded
       int startInitial = 0;
       int endInitial = 3;
       int endInvestigation = 55;
       
       // get initial configuration
       // empty configuration
       getInitialConfiguration(startInitial, endInitial);
       
       // set up the new workload
       List<SQLStatement> sqls = new ArrayList<SQLStatement>();
       for (int i = startInitial; i <= endInvestigation; i++)
           sqls.add(wlOnline.get(i));
       workload = new Workload(sqls);
       
       List<QueryPlanDesc> descs = onlineDiv.getQueryPlanDescs(startInitial, endInvestigation);
       
       // 2. Get the total deployment cost
       double reConfigurationCost = 0.0;
       DivBIPFunctionalTest.testDiv(nReplicas, B, descs);
       reConfigurationCost = divConf.transitionCost(initialConf, true);
       Rt.p("Reconfiguration costs: " + reConfigurationCost);
       // Add a feasibility check
       
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
        Rt.p("Initial configuration: " + workload.size());
        DivBIPFunctionalTest.testDiv(nReplicas, B, true);
        initialConf = divConf;
    }
}
