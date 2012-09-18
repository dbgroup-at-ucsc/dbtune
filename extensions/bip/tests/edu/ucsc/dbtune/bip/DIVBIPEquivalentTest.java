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
        
        // 2. special data structures for this class
        entries = new HashMap<DivPaperEntry, Double>();
        divFile = new File(rawDataDir, DIV_DB2_FILE);
        
        // experiment for DIV equivalent to BIP
        for (int i = 0; i < dbNames.length; i++) 
            runExpts(dbNames[i], wlNames[i]);
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
    public static void runExpts(String dbName, String wlName)
                       throws Exception
    {
        //get database instances, candidate indexes
        getEnvironmentParameters(dbName, wlName);

        // get parameters
        setParameters();
        
        double costBip;
        
        for (double B : listBudgets) 
            for (int n : listNumberReplicas) {
                costBip = DivBIPFunctionalTest.testDivSimplify(n, B, false);
                DivPaperEntry entry = new DivPaperEntry
                        (dbName, wlName, n, (long) B);
                
                entries.put(entry, costBip);
            }
        
        
        // store in the serialize file
        serializeDivResult(entries, divFile);
        
        // test the result
        entries = readDivResult(divFile);
        Rt.p(" result " + entries);
    }
}
