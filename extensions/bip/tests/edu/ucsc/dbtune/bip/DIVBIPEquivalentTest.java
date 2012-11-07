package edu.ucsc.dbtune.bip;

import java.io.File;
import java.util.HashMap;
import org.junit.Test;

import edu.ucsc.dbtune.bip.util.LogListener;
import edu.ucsc.dbtune.util.Rt;

/**
 * Test basic DIT
 * @author Quoc Trung Tran
 *
 */
public class DIVBIPEquivalentTest extends DIVPaper 
{   
    public static String fileName;
    
    @Test
    public void main() throws Exception
    {
        // 1. initialize file locations & necessary 
        // data structures
        initialize();
        
        // get parameters
        getEnvironmentParameters();        
        setParameters();
        fileName = wlName + "_" + DIV_DETAIL_DB2_FILE;
        
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
        long budget;
        timeBIP = 0.0;
        
        boolean isOnTheFly = false;
        WorkloadCostDetail wc;
        detailEntries = new HashMap<DivPaperEntryDetail, Double>();
        for (double B : listBudgets) 
            for (int n : listNumberReplicas) {
                LogListener logger = LogListener.getInstance();
                wc = DivBIPFunctionalTest.testDivSimplify(n, B, 
                                    isOnTheFly, logger);
                budget = convertBudgetToMB(B);
                DivPaperEntryDetail entry = new DivPaperEntryDetail
                        (dbName, wlName, n, budget, divConf);
                entry.queryCost = wc.queryCost;
                entry.updateCost = wc.updateCost;
                
                detailEntries.put(entry, wc.totalCost);
                timeBIP += logger.getTotalRunningTime();
                
                // 2. store in the file
                divFile = new File(rawDataDir, fileName);
                divFile.delete();
                divFile = new File(rawDataDir, fileName);
                divFile.createNewFile();
                
                // store in the serialize file
                serializeDivResultDetail(detailEntries, divFile);
            }
        
        double averageBIP = timeBIP / (listBudgets.size()
                                        * listNumberReplicas.size());
        Rt.p("TOTAL running time = " + timeBIP
                + " AVERGE Running time = " + averageBIP);
    }
}
