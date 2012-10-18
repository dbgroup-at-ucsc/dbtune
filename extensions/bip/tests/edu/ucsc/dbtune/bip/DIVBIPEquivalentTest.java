package edu.ucsc.dbtune.bip;

import java.io.File;
import java.util.HashMap;
import org.junit.Test;

import edu.ucsc.dbtune.bip.util.LogListener;
import edu.ucsc.dbtune.util.Rt;


public class DIVBIPEquivalentTest extends DIVPaper 
{   
    @Test
    public void main() throws Exception
    {
        // 1. initialize file locations & necessary 
        // data structures
        initialize();
        
        // get parameters
        getEnvironmentParameters();        
        setParameters();
        
        // experiment for DIV equivalent to BIP    
        runDivBIP();
        
        // not to draw graph
        resetParameterNotDrawingGraph();
    }
    
    /**
     * Call DIVBIP for each pair of replicas and budgets 
     * 
     * 
     * @throws Exception
     */
    public static void runDivBIP() throws Exception
    {   
        // run algorithms
        double costBip;
        long budget;
        timeBIP = 0.0;
        
        boolean isOnTheFly = false;
        
        entries = new HashMap<DivPaperEntry, Double>();
        for (double B : listBudgets) 
            for (int n : listNumberReplicas) {
                LogListener logger = LogListener.getInstance();
                costBip = DivBIPFunctionalTest.testDivSimplify(n, B, 
                                    isOnTheFly, logger);
                budget = convertBudgetToMB(B);
                DivPaperEntry entry = new DivPaperEntry
                        (dbName, wlName, n, budget, divConf);
                
                entries.put(entry, costBip);
                timeBIP += logger.getTotalRunningTime();
                
                // 2. store in the file
                divFile = new File(rawDataDir, wlName + "_" + DIV_DB2_FILE);
                divFile.delete();
                divFile = new File(rawDataDir, wlName + "_" + DIV_DB2_FILE);
                divFile.createNewFile();
                
                // store in the serialize file
                serializeDivResult(entries, divFile);
            }
        
        double averageBIP = timeBIP / (listBudgets.size()
                                        * listNumberReplicas.size());
        Rt.p("TOTAL running time = " + timeBIP
                + " AVERGE Running time = " + averageBIP);
    }
}
