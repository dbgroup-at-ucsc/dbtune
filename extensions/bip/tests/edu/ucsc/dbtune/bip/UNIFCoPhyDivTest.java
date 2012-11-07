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
    /*
    @Test
    public void main() throws Exception
    {
        // 1. initialize data structures, like database names
        // wl names
        initialize();
        
        // 3. for each (dbname, wlname) derive 
        // the UNIF cost
        entries = new HashMap<DivPaperEntry, Double>();
        testUNIFCoPhy();
        
        // 2. special data structures for this class
        
        unifCoPhyFile = new File(rawDataDir, wlName + "_" + UNIF_COPHY_FILE);
        unifCoPhyFile.delete();
        unifCoPhyFile = new File(rawDataDir, wlName + "_" + UNIF_COPHY_FILE);
        unifCoPhyFile.createNewFile();
        
        // store in the serialize file
        serializeDivResult(entries, unifCoPhyFile);
        
        // test the result
        entries = readDivResult(unifCoPhyFile);
        Rt.p(" result UNIF COPHY " + entries);
        
     // not to draw graph
        resetParameterNotDrawingGraph();
    }
    */
    
    
    /**
     * Run UNIFCOPHY on the given database and workload
     * 
     * @param dbName
     *      Database
     * @param workloadName
     *      Workload
     * @throws Exception
     */
    /*
    public static void testUNIFCoPhy() throws Exception
    {   
        // 1. Parameter
        getEnvironmentParameters();
        setParameters();        
        
        // 2. Running algorithms
        List<Double> totalCosts;
        long budget;
        for (double B1 : listBudgets) { 
            totalCosts = DivBIPFunctionalTest.testUniformSimplify
                                (listNumberReplicas, B1);
            Rt.p(" space budget = " + B1 + " " + totalCosts);
            
            for (int i = 0; i < listNumberReplicas.size(); i++){
                budget = convertBudgetToMB(B1);
                DivPaperEntry entry = new DivPaperEntry
                (dbName, wlName, listNumberReplicas.get(i), budget, null);
                
                entries.put(entry, totalCosts.get(i));
            }
        }
    }
    */
}
