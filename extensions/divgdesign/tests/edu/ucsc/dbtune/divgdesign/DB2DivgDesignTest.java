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
        
        long start = System.currentTimeMillis();
        
        // 3. 
        entries = new HashMap<DivPaperEntry, Double>();
        testDivgDesignDB2();
        
        long end = System.currentTimeMillis();
        long totalTimes= (end - start) / 1000; // in secs
        Rt.p(" Total running time" + totalTimes
                + " avg: " + totalTimes / (listBudgets.size() * listNumberReplicas.size()));
        // 2. special data structures for this class
        storeResult();
        
    }
    
    protected static void storeResult() throws Exception
    {
        designFile = new File(rawDataDir, wlName + "_" + DESIGN_DB2_FILE);
        designFile.delete();
        designFile = new File(rawDataDir, wlName + "_" + DESIGN_DB2_FILE);
        designFile.createNewFile();
        
        // store in the serialize file
        serializeDivResult(entries, designFile);
        
        // test the result
        entries = readDivResult(designFile);
        Rt.p(" result " + entries);
        
        // not to draw graph
        resetParameterNotDrawingGraph();
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
    public static void testDivgDesignDB2() throws Exception
    {   
        // 1. Read common parameter
        getEnvironmentParameters();
        setParameters();
        
        // 2. Run algorithms
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
                        (dbName, wlName, n, budget, null);
                
                entries.put(entry, costDiv);
                // since it is expensive
                // store after one result is obtained
                storeResult();
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
        
        divConf = design.getRecommendation();
        Rt.p("DESIGN cost: n = " + n + " B = " + B
                + " average cost = " + (totalCost / numIters));
        return totalCost / numIters;
    }
}
