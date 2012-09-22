package edu.ucsc.dbtune.bip;

import java.io.File;
import java.util.HashMap;
import java.util.List;

import org.junit.Test;
import edu.ucsc.dbtune.util.Rt;

/**
 * Class to test UNIF and DIVGDESIGN using CoPhy
 * 
 * @author Quoc Trung Tran
 *
 */
public class UNIFCoPhyDivTest extends DIVPaper
{
    @Test
    public void main() throws Exception
    {
        // 1. initialize data structures, like database names
        // wl names
        initialize();
        
        // 2. special data structures for this class
        testUNIFCoPhy();
    }
    
    /**
     * Test UNIF using CoPhy
     * @throws Exception
     */
    protected static void testUNIFCoPhy() throws Exception
    {
        entries = new HashMap<DivPaperEntry, Double>();
        unifCoPhyFile = new File(rawDataDir, UNIF_COPHY_FILE);
        unifCoPhyFile.delete();
        unifCoPhyFile = new File(rawDataDir, UNIF_COPHY_FILE);
        unifCoPhyFile.createNewFile();
        
        // 3. for each (dbname, wlname) derive 
        // the UNIF cost
        for (int i = 0; i < dbNames.length; i++) 
            testUNIFCoPhy(dbNames[i], wlNames[i]);
        
        // store in the serialize file
        serializeDivResult(entries, unifCoPhyFile);
        
        // test the result
        entries = readDivResult(unifCoPhyFile);
        Rt.p(" result UNIF COPHY " + entries);
    }
    
    /**
     * Run UNIFCOPHY on the given database and workload
     * 
     * @param dbName
     *      Database
     * @param workloadName
     *      Workload
     * @throws Exception
     */
    public static void testUNIFCoPhy(String dbName, String workloadName)
                throws Exception
    {   
        // 1. Read common parameter
        getEnvironmentParameters(dbName, workloadName);
        
        // 2. set parameter for DivBIP()
        setParameters();        
        List<Double> totalCosts;
        long budget;
        for (double B1 : listBudgets) { 
            totalCosts = DivBIPFunctionalTest.testUniformSimplify
                                (listNumberReplicas, B1);
            Rt.p(" space budget = " + B1 + " " + totalCosts);
            
            for (int i = 0; i < listNumberReplicas.size(); i++){
                budget = convertBudgetToMB(B1);
                DivPaperEntry entry = new DivPaperEntry
                (dbName, workloadName, listNumberReplicas.get(i), budget);
                
                entries.put(entry, totalCosts.get(i));
            }
        }
    }
}
