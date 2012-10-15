package edu.ucsc.dbtune.divgdesign;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.junit.Test;

import edu.ucsc.dbtune.divgdesign.CoPhyDivgDesign;


import edu.ucsc.dbtune.bip.DIVPaper;
import edu.ucsc.dbtune.bip.DivBIPFunctionalTest;
import edu.ucsc.dbtune.bip.core.QueryPlanDesc;
import edu.ucsc.dbtune.bip.util.LogListener;
import edu.ucsc.dbtune.optimizer.InumOptimizer;
import edu.ucsc.dbtune.util.Rt;


/**
 * Test the usage of CoPhy in DivgDesign
 * 
 * @author Quoc Trung Tran
 *
 */
public class CoPhyDivgDesignFunctionalTest extends DIVPaper
{   
    private static int maxIters;
    private static List<QueryPlanDesc> descs;
    private static CoPhyDivgDesign divg;
    
    @Test
    public void main() throws Exception
    {
        // 1. initialize data structures, like database names
        // wl names
        initialize();
        
        // 2. special data structures for this class
        entries = new HashMap<DivPaperEntry, Double>();
        isCoPhyDesign = true;
        
        // Run algorithms
        testDivgDesignCoPhy();
        
        designCoPhyFile = new File(rawDataDir, wlName + "_" + DESIGN_COPHY_FILE);
        designCoPhyFile.delete();
        designCoPhyFile = new File(rawDataDir, wlName + "_" + DESIGN_COPHY_FILE);
        designCoPhyFile.createNewFile();
        
        long end = System.currentTimeMillis();
        long totalTimes= (end - start) / 1000; // in secs
        Rt.p(" Total running time" + totalTimes
                + " avg: " + totalTimes / (listBudgets.size() * listNumberReplicas.size()));
        // store in the serialize file
        serializeDivResult(entries, designCoPhyFile);
        
        // test the result
        entries = readDivResult(designCoPhyFile);
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
    public static void testDivgDesignCoPhy() throws Exception
    {   
        // 1. Read common parameter
        getEnvironmentParameters();
        setParameters();
        readQueryPlanDescs();
        
        // 2. Run algorithms
        double costDiv;
        long budget;
        for (double B : listBudgets) 
            for (int n : listNumberReplicas) {
                costDiv = testDivgDesign(n, B);
                budget = convertBudgetToMB(B);
                DivPaperEntry entry = new DivPaperEntry
                        (dbName, wlName, n, budget, null);
                
                entries.put(entry, costDiv);
                
                designCoPhyFile = new File(rawDataDir, wlName + "_" + DESIGN_COPHY_FILE);
                designCoPhyFile.delete();
                designCoPhyFile = new File(rawDataDir, wlName + "_" + DESIGN_COPHY_FILE);
                designCoPhyFile.createNewFile();
                
                // store in the serialize file
                serializeDivResult(entries, designCoPhyFile);
            }
    }
    
    
    /**
     * Run the CoPhyDiv algorithm.
     * 
     * @throws Exception
     */
    public static double testDivgDesign(int _n, double _B) throws Exception
    {
        Rt.p("TEST DIVGDESIGN COPHY, n = " + _n 
             + " space = " + _B);
        maxIters = 3;
        
        B = _B;
        nReplicas = _n;
        loadfactor = (int) Math.ceil( (double) nReplicas / 2);
        LogListener logger; 
        List<CoPhyDivgDesign> divgs = new ArrayList<CoPhyDivgDesign>();
        
        // run at most {@code maxIters} times
        int minPosition = -1;
        double minCost = -1;
        double avgReplicaImbalance = 0.0;
        
        for (int iter = 0; iter < maxIters; iter++) {
            logger = LogListener.getInstance();
            divg = new CoPhyDivgDesign(db, (InumOptimizer) io, logger, descs);
            divg.recommend(workload, nReplicas, loadfactor, B);
            divgs.add(divg);
            //costDivgDB2 = convertToDB2Cost(divg);
            
            if (iter == 0 || minCost > divg.getTotalCost()) {
                minPosition = iter;
                minCost = divg.getTotalCost();
            } 
            
            avgReplicaImbalance += divg.getImbalanceReplica();
        }
        
        avgReplicaImbalance /= maxIters;
        
        // get the best among these runs
        double timeAnalysis = 0.0;
        double timeInum = 0.0;
        for (CoPhyDivgDesign div : divgs) {
            timeInum += div.getInumTime();
            timeAnalysis += div.getAnalysisTime();
            Rt.p("cost INUM = " + div.getTotalCost()
                    + " QUERY cost = " + div.getQueryCost() 
                            + " Number of iterations: " + div.getNumberOfIterations()
                            + " INUM time : " + div.getInumTime()
                            + " Analysis time: " + div.getAnalysisTime());
            
        }
        
        divg = divgs.get(minPosition);
        divConf = divg.getRecommendation();
        // compute minCost in terms of DB2 cost
        minCost = convertToDB2Cost(divg);
        
        Rt.p("CoPhy Divergent Design \n"
                            + " INUM time: " + timeInum + "\n"
                            + " ANALYSIS time: " + timeAnalysis + "\n"
                            + " TOTAL running time: " + (timeInum + timeAnalysis) + "\n"
                            + " The objective value: " + minCost + "\n"
                            + " REPLICA IMBALANCE: " + avgReplicaImbalance + "\n"
                             );
        
        return minCost;
    }
    
    /**
     * convert into db2 cost
     * @param divg
     * @return
     */
    public static double convertToDB2Cost(CoPhyDivgDesign divg) throws Exception
    {
        double queryCost = DivBIPFunctionalTest.getDB2QueryCostDivConf(divg.getRecommendation());
        return (queryCost + divg.getUpdateCost());        
    }
    
    @SuppressWarnings("unchecked")
    protected static void readQueryPlanDescs() throws Exception
    {
        String fileName = en.getWorkloadsFoldername() + "/query-plan-desc.bin";
        File file = new File(fileName);
        ObjectInputStream in;
        
        try {
            FileInputStream fileIn = new FileInputStream(file);
            in = new ObjectInputStream(fileIn);
            descs = (List<QueryPlanDesc>) in.readObject();
            
            // reassign the statement with the corresponding weight
            for (int i = 0; i < descs.size(); i++)
                descs.get(i).setStatement(workload.get(i));
        
            in.close();
            fileIn.close();
        } catch(IOException e) {
            throw new SQLException(e);
        } catch (ClassNotFoundException e) {
            throw new SQLException(e);
        }
    }
}
