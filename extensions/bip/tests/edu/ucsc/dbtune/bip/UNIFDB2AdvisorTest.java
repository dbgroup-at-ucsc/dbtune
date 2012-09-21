package edu.ucsc.dbtune.bip;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.junit.Test;
import edu.ucsc.dbtune.util.Rt;

public class UNIFDB2AdvisorTest extends DIVPaper 
{   
    @Test
    public void main() throws Exception
    {
        // 1. initialize data structures, like database names
        // wl names
        initialize();
        
        // 2. special data structures for this class
        entries = new HashMap<DivPaperEntry, Double>();
        unifFile = new File(rawDataDir, UNIF_DB2_FILE);
        unifFile.delete();
        unifFile = new File(rawDataDir, UNIF_DB2_FILE);
        unifFile.createNewFile();
        
        
        // 3. for each (dbname, wlname) derive 
        // the UNIF cost
        for (int i = 0; i < dbNames.length; i++) 
            testUNIFDB2(dbNames[i], wlNames[i]);
        
        // store in the serialize file
        serializeDivResult(entries, unifFile);
        
        // test the result
        entries = readDivResult(unifFile);
        Rt.p(" result " + entries);
    }
    

    /**
     * Run UNIFDB2 on the given databaes and workload
     * 
     * @param dbName
     *      Database
     * @param workloadName
     *      Workload
     * @throws Exception
     */
    public static void testUNIFDB2(String dbName, String workloadName)
                throws Exception
    {   
        // 1. Read common parameter
        getEnvironmentParameters(dbName, workloadName);
        
        // 2. set parameter for DivBIP()
        setParameters();        
        List<Double> totalCosts;
        long budget;
        for (double B1 : listBudgets) { 
            totalCosts = testUniformDB2Advis(listNumberReplicas, B1);
            Rt.p(" space budget = " + B1 + " " + totalCosts);
            
            for (int i = 0; i < listNumberReplicas.size(); i++){
                budget = convertBudgetToMB(B1);
                DivPaperEntry entry = new DivPaperEntry
                (dbName, workloadName, listNumberReplicas.get(i), budget);
                
                entries.put(entry, totalCosts.get(i));
            }
        }
    }
    
    /**
     * Computer UNIF total cost by using DB2Advisor directly
     * 
     * @param listNReplicas
     *      List of number of replicas
     * @param B1
     *      Space budget constraint
     *      
     * @return
     *      List of total cost for each replica
     * @throws Exception
     */
    public static List<Double> testUniformDB2Advis(List<Integer> listNReplicas, double B1)
            throws Exception
    {
        List<Double> costs = new ArrayList<Double>();
        double totalCost = 0.0;
        db2Advis.process(workload);
        int budget = (int) (B1 / Math.pow(2, 20));
        for (double cost : computeQueryCostsDB2(workload, db2Advis.getRecommendation(budget)))
            totalCost += cost;
        
        for (int i=0; i<listNReplicas.size(); i++)
            costs.add(totalCost);
        Rt.p("UNIF uing DB2Advis, space budget = " + B1
                + " UNIF = " + costs);
        return costs;
    }
}
