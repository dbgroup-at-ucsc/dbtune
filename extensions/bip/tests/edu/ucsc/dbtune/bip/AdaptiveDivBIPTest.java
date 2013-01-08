package edu.ucsc.dbtune.bip;

import java.util.ArrayList;
import java.util.List;

import edu.ucsc.dbtune.bip.core.QueryPlanDesc;
import edu.ucsc.dbtune.bip.div.DivBIP;
import edu.ucsc.dbtune.bip.div.DivConfiguration;
import edu.ucsc.dbtune.bip.div.OnlineDivBIP;
import edu.ucsc.dbtune.bip.util.LogListener;
import edu.ucsc.dbtune.optimizer.InumOptimizer;
import edu.ucsc.dbtune.optimizer.Optimizer;
import edu.ucsc.dbtune.util.Rt;
import edu.ucsc.dbtune.workload.SQLStatement;
import edu.ucsc.dbtune.workload.Workload;

public class AdaptiveDivBIPTest extends DIVPaper 
{
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
     * Get initial configuration for the system
     * @throws Exception
     */
    protected static void getInitialConfiguration(int startID, int endID) 
                    throws Exception
    {
        // empty INITIAL configuration
        if (endID == -1){
            Rt.p(" EMPTY INTIAL CONFIGURATION");
            initializeDivBIP();
            
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
        
    /**
     * Initial DIVBIP object
     * @throws Exception
     */
    protected static DivBIP initializeDivBIP() throws Exception
    {
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
        
        return div;
    }
    
}
