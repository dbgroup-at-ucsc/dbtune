package edu.ucsc.dbtune.bip;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Set;

import org.junit.Test;

import edu.ucsc.dbtune.bip.div.DivConfiguration;
import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.util.Rt;

import static edu.ucsc.dbtune.workload.SQLCategory.SELECT;

public class UNIFDB2AdvisorTest extends DIVPaper 
{   
    @Test
    public void main() throws Exception
    {
        // 1. initialize data structures, like database names
        // wl names
        initialize();
        
        // 3. for each (dbname, wlname) derive 
        // the UNIF cost
        testUNIFDB2();
        
        // not to draw graph
        resetParameterNotDrawingGraph();
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
    public static void testUNIFDB2() throws Exception
    {   
        // 1. Read common parameter
        getEnvironmentParameters();
                
        // 2. set parameter for DivBIP()
        setParameters();        
        List<Double> totalCosts;
        long budget;
        entries = new HashMap<DivPaperEntry, Double>();
        
        for (double B1 : listBudgets) { 
            totalCosts = testUniformDB2Advis(listNumberReplicas, B1);
            Rt.p(" space budget = " + B1 + " " + totalCosts);
            
            for (int i = 0; i < listNumberReplicas.size(); i++){
                budget = convertBudgetToMB(B1);
                DivPaperEntry entry = new DivPaperEntry
                (dbName, wlName, listNumberReplicas.get(i), budget, divConf);
                
                entries.put(entry, totalCosts.get(i));
            }
            
            unifFile = new File(rawDataDir, wlName + "_" + UNIF_DB2_FILE);
            unifFile.delete();
            unifFile = new File(rawDataDir, wlName + "_" + UNIF_DB2_FILE);
            unifFile.createNewFile();
            
            // store in the serialize file
            serializeDivResult(entries, unifFile);
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
        List<Double> costs;
        double totalCost = 0.0;
        db2Advis.process(workload);
        int budget = (int) (B1 / Math.pow(2, 20));
        Set<Index> recommendation = db2Advis.getRecommendation(budget);
        
        // create a configuration
        divConf = new DivConfiguration(1, 1);
        for (Index index : recommendation)
            divConf.addIndexReplica(0, index);
        
        costs = computeCostsDB2(workload, recommendation);
        double queryCost = 0.0;
        double updateCost = 0.0;
        
        for (int i = 0; i < workload.size(); i++) {
            if (workload.get(i).getSQLCategory().equals(SELECT))
                queryCost += costs.get(i) * workload.get(i).getStatementWeight();
            else 
                updateCost += costs.get(i) * workload.get(i).getStatementWeight();
        }
        
        Rt.p("DB2-UNIF, space = " + B1
                + " query cost = " + queryCost
                + " updat cost = " + updateCost);
         
        costs = new ArrayList<Double>();
        for (int i=0; i < listNReplicas.size(); i++) {
            totalCost = queryCost + listNReplicas.get(i) * updateCost;
            costs.add(totalCost);
        }
        Rt.p("UNIF uing DB2Advis, space budget = " + B1
                + " UNIF = " + costs);
        return costs;
    }
}
