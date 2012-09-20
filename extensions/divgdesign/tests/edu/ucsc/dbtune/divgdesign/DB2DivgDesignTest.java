package edu.ucsc.dbtune.divgdesign;


import java.io.File;
import java.util.HashMap;
import org.junit.Test;

import edu.ucsc.dbtune.bip.DIVPaper;
import edu.ucsc.dbtune.optimizer.DB2Optimizer;
import edu.ucsc.dbtune.optimizer.Optimizer;
import edu.ucsc.dbtune.util.Rt;


public class DB2DivgDesignTest extends DIVPaper
{   
    protected static DB2DivgDesign design;
    
    @Test
    public void main() throws Exception
    {
        // 1. initialize data structures, like database names
        // wl names
        initialize();
        
        // 2. special data structures for this class
        entries = new HashMap<DivPaperEntry, Double>();
        designFile = new File(rawDataDir, DESIGN_DB2_FILE);
        
        long start = System.currentTimeMillis();
        
        // 3. for each (dbname, wlname) derive 
        // the UNIF cost
        for (int i = 0; i < dbNames.length; i++) 
            testDivgDesignDB2(dbNames[i], wlNames[i]);
        
        long end = System.currentTimeMillis();
        long totalTimes= (end - start) / 1000; // in secs
        Rt.p(" Total running time" + totalTimes
                + " avg: " + totalTimes / (listBudgets.size() * listNumberReplicas.size()));
        // store in the serialize file
        serializeDivResult(entries, designFile);
        
        // test the result
        entries = readDivResult(designFile);
        Rt.p(" result " + entries);
    }
    

    /**
     * Run UNIFDB2 on the given databaes and workload
     * 
     * @param dbName
     *      Database
     * @param wlName
     *      Workload
     * @throws Exception
     */
    public static void testDivgDesignDB2(String dbName, String wlName)
                throws Exception
    {   
        // 1. Read common parameter
        getEnvironmentParameters(dbName, wlName);
        
        // 2. set parameter for DivBIP()
        setParameters();
        
        Optimizer io = db.getOptimizer();
        DB2Optimizer db2Opt = (DB2Optimizer) io.getDelegate();
        design = new DB2DivgDesign(db2Advis, db2Opt);
        
        double costDiv;
        long budget;
        for (double B : listBudgets) 
            for (int n : listNumberReplicas) {
                costDiv = testDivgDesign(n, B);
                budget = convertBudgetToMB(B);
                DivPaperEntry entry = new DivPaperEntry
                        (dbName, wlName, n, budget);
                
                entries.put(entry, costDiv);
            }
    }
    
    /**
     * Run DIVGDESGIN
     * @param n
     *      Number of replicas
     *      
     * @param B
     *      Space budget
     *      
     * @return
     *      The total cost
     */
    protected static double testDivgDesign(int n, double B) throws Exception
    {
        loadfactor = (int) Math.ceil( (double) n / 2); 
        int budget = (int) (B / Math.pow(2, 20)); // in MB
        double totalCost = 0;
        int numIters = 3;
        
        for (int i = 0; i < numIters; i++){
            design.recommend(workload, n, loadfactor, budget);
            totalCost +=  design.getTotalCost();
        }
        
        return totalCost / numIters;
    }
}