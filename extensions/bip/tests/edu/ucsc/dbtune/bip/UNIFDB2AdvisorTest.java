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
    public static String fileName;
    
    @Test
    public void main() throws Exception
    {
        // 1. initialize data structures, like database names
        // wl names
        initialize();
        
        // 3. for each (dbname, wlname) derive 
        // the UNIF cost
        getEnvironmentParameters();
        setParameters();
        fileName = wlName + "_" + UNIF_DETAIL_DB2_FILE;
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
        List<WorkloadCostDetail> wcs;
        long budget;
        detailEntries = new HashMap<DivPaperEntryDetail, Double>();
        
        for (double B1 : listBudgets) { 
            wcs = testUniformDB2Advis(listNumberReplicas, B1);
            Rt.p(" space budget = " + B1 + " " + wcs);
            
            for (int i = 0; i < listNumberReplicas.size(); i++){
                budget = convertBudgetToMB(B1);
                DivPaperEntryDetail entry = new DivPaperEntryDetail
                (dbName, wlName, listNumberReplicas.get(i), budget, divConf);
                entry.queryCost = wcs.get(i).queryCost;
                entry.updateCost = wcs.get(i).updateCost;
                
                detailEntries.put(entry, wcs.get(i).totalCost);
            }
            
            Rt.p(" store in file name = " + fileName);
            unifFile = new File(rawDataDir, fileName);
            unifFile.delete();
            unifFile = new File(rawDataDir, fileName);
            unifFile.createNewFile();
            
            // store in the serialize file
            serializeDivResultDetail(detailEntries, unifFile);
        }
        
        Rt.p(" The design is in object divConf ");
        Rt.p("Detail = " + divConf);
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
    public static List<WorkloadCostDetail> testUniformDB2Advis(List<Integer> listNReplicas, double B1)
            throws Exception
    {   
        db2Advis.process(workload);
        int budget = (int) (B1 / Math.pow(2, 20));
        Set<Index> recommendation = db2Advis.getRecommendation(budget);
        
        // create a configuration
        divConf = new DivConfiguration(1, 1);
        for (Index index : recommendation)
            divConf.addIndexReplica(0, index);
        
        List<Double> costs = computeCostsDB2(workload, recommendation);
        double queryCost = 0.0;
        double updateCost = 0.0;
        
        for (int i = 0; i < workload.size(); i++)
            if (workload.get(i).getSQLCategory().isSame(SELECT))
                queryCost += costs.get(i) * workload.get(i).getStatementWeight();
            else
                updateCost += costs.get(i) * workload.get(i).getStatementWeight();
        
         
        List<WorkloadCostDetail> wcs = new
                ArrayList<WorkloadCostDetail>();
        WorkloadCostDetail wc;
        for (int i=0; i < listNReplicas.size(); i++) {
            wc = new WorkloadCostDetail();
            wc.queryCost = queryCost;
            wc.updateCost = listNReplicas.get(i) * updateCost;
            wc.totalCost = wc.queryCost + wc.updateCost;
            wcs.add(wc);
        }
        
        return wcs;
    }
}
