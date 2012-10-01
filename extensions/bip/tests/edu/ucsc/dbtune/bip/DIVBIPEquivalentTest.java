package edu.ucsc.dbtune.bip;

import java.io.File;
import java.util.HashMap;
import org.junit.Test;

import edu.ucsc.dbtune.util.Rt;


public class DIVBIPEquivalentTest extends DIVPaper 
{   
    @Test
    public void main() throws Exception
    {
        // 1. initialize file locations & necessary 
        // data structures
        initialize();
        
        // experiment for DIV equivalent to BIP        
        entries = new HashMap<DivPaperEntry, Double>();
        runExpts();
        
        // 2. special data structures for this class
        
        divFile = new File(rawDataDir, wlName + "_" + DIV_DB2_FILE);
        divFile.delete();
        divFile = new File(rawDataDir, wlName + "_" + DIV_DB2_FILE);
        divFile.createNewFile();
        
        // store in the serialize file
        serializeDivResult(entries, divFile);
        
        // test the result
        entries = readDivResult(divFile);
        Rt.p(" result " + entries);
    }
    
    /**
     * Call DIVBIP for each pair of replicas and budgets 
     * 
     * @param dbName
     *      Database name
     * @param wlName
     *      Workload name
     * @throws Exception
     */
    public static void runExpts() throws Exception
    {
        // get parameters
        getEnvironmentParameters();        
        setParameters();
        
        // run algorithms
        double costBip;
        long budget;
        for (double B : listBudgets) 
            for (int n : listNumberReplicas) {
                costBip = DivBIPFunctionalTest.testDivSimplify(n, B, false);
                budget = convertBudgetToMB(B);
                DivPaperEntry entry = new DivPaperEntry
                        (dbName, wlName, n, budget);
                
                entries.put(entry, costBip);
            }
    }
}
