package edu.ucsc.dbtune.bip;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;

import org.junit.Test;

import edu.ucsc.dbtune.bip.core.IndexTuningOutput;
import edu.ucsc.dbtune.bip.div.RobustDivBIP;
import edu.ucsc.dbtune.bip.util.LogListener;
import edu.ucsc.dbtune.optimizer.InumOptimizer;
import edu.ucsc.dbtune.util.Rt;

import static edu.ucsc.dbtune.bip.div.UtilConstraintBuilder.computeCoefCostWithoutFailure;
import static edu.ucsc.dbtune.bip.div.UtilConstraintBuilder.computeCoefCostWithFailure;

public class RobustDivBIPTest extends DIVPaper 
{   
    @Test
    public void testConstraintDiv() throws Exception
    {
        // 1. Set common parameters
        initialize();
        getEnvironmentParameters();
        setParameters();
        
        List<Double> imbalanceFactors = new ArrayList<Double>();
        List<Double> failureFactors = new ArrayList<Double>();
        
        // get the imbalance factors from inputs
        try {   
            imbalanceFactors = en.getNodeImbalanceFactors();
        }
        catch (NoSuchElementException e){
            ;
        }
        
        try {   
            failureFactors = en.getFailureImbalanceFactors();
        }
        catch (NoSuchElementException e){
            ;
        }
        
        if (imbalanceFactors.size() != failureFactors.size()){
            Rt.p(" List of imbalance factors must have the same size "
                    + " with list of failure factors");
            System.exit(1);
        }
            
        // run the test
        testFailureImbalance(failureFactors, imbalanceFactors, true);

        // not to draw graph
        resetParameterNotDrawingGraph();
    }
    

    /**
     * Test with failure only
     * 
     * @param failureFactors
     * @throws Exception
     */
    protected static void testFailureImbalance(List<Double> failureFactors,
            List<Double> nodeFactors, boolean isGreedy) throws Exception
    {
        RobustDivBIP robustDiv;
        double optimalTotalCost;
        List<RobustPaperEntry> entries;
        
        optimalTotalCost = DivBIPFunctionalTest.testDiv(nReplicas, B, false);
        div.clear();
        Rt.p(" optimal total cost = " + optimalTotalCost);
        entries = new ArrayList<RobustPaperEntry>();

        String name = FAILURE_IMBALANCE_GREEDY_FILE;
        
        for (int i = 0; i < nodeFactors.size(); i++){

            robustDiv = new RobustDivBIP(optimalTotalCost, nodeFactors.get(i), 
                             failureFactors.get(i));
            
            imbalanceFile = new File(rawDataDir, wlName + "_" + name);
            imbalanceFile.delete();
            imbalanceFile = new File(rawDataDir, wlName + "_" + name);
            imbalanceFile.createNewFile();
            runFullDDT(robustDiv, isGreedy, entries);
            
            // ----------------------------------
            // store to file (since it takes time)
            serializeFailureImbalanceResult(entries, imbalanceFile);
        }
    }
    
    
    
    
    /**
     * Handle both load-imbalance and failures
     * 
     * @throws Exception
     */
    public static void runFullDDT(RobustDivBIP robustDiv, boolean isGreedy, 
                                List<RobustPaperEntry> entries) 
            throws Exception
    {   
        double totalCostBIP = -1.0;
        io = db.getOptimizer();
        
        LogListener logger = LogListener.getInstance();
        robustDiv.setCandidateIndexes(candidates);
        robustDiv.setWorkload(workload); 
        robustDiv.setOptimizer((InumOptimizer) io);
        robustDiv.setNumberReplicas(nReplicas);
        robustDiv.setLoadBalanceFactor(loadfactor);
        robustDiv.setSpaceBudget(B);
        robustDiv.setLogListenter(logger);
        
        IndexTuningOutput output = robustDiv.solve();
        Rt.p(logger.toString());
        double time = logger.getTotalRunningTime();
        
        if (output != null) {
            double costUnifUnderFailure = computeCostUNIFUnderFailure (robustDiv.getFailureFactor(), 
                    nReplicas);
            totalCostBIP = robustDiv.computeOptimizerCost(); 
            
            Rt.p(" ------------- \n"
                    + " Number of replicas = " + nReplicas 
                    + " space =  " + B + "\n" 
                    + " TOTAL COST (INUM) = " + robustDiv.getObjValue() + "\n"
                    + " TOTAL COST (in optimizer units) = " + totalCostBIP 
                    + " TOTAL UNIF = " + costUnifUnderFailure
                    + " DIVBIP / UNIF = " + (totalCostBIP / costUnifUnderFailure)
                );
            
            RobustPaperEntry entry = new RobustPaperEntry
                    (robustDiv.getFailureFactor(), robustDiv.getNodeFactor(),
                            totalCostBIP, costUnifUnderFailure, time);
            entries.add(entry);
            
            if (isShowRecommendation)
                Rt.p(" solution: " + output);
            
            // show imbalance query & replica
            Rt.p(" NODE IMBALANCE: " + robustDiv.getNodeImbalance());
            
        } else 
            Rt.p(" NO SOLUTION ");
        
        
    }
    
    /**
     * Get the cost under failure
     * 
     * @param factor
     * @param N
     * @return
     * @throws Exception
     */
    protected static double computeCostUNIFUnderFailure(double factor, int N)
            throws Exception
    {
        double costUnifUnderFailure = 1.0;
        double costUnif;
        //  1. Read the result from UNIF file
        unifFile = new File(rawDataDir, wlName + "_" + UNIF_DB2_FILE);
        Rt.p(" file name = " + unifFile.getName()
                + " absolute path = "
                + unifFile.getAbsolutePath());
        mapUnif = readDivResult(unifFile);
    
        DivPaperEntry entry = new DivPaperEntry(dbName, wlName, nReplicas, convertBudgetToMB (B), null);
        if (!mapUnif.containsKey(entry))
            entry = new DivPaperEntry(dbName.toLowerCase(), wlName, nReplicas, convertBudgetToMB (B), null);
        
        costUnif = mapUnif.get(entry);
        costUnifUnderFailure = 
            (computeCoefCostWithoutFailure(factor, N)
                    + computeCoefCostWithFailure(factor, N))
                    * costUnif;
        Rt.p(" COST UNIF = " + costUnif
                + " COST UNIF UNDER FAILURE = " + costUnifUnderFailure);
        
        return costUnifUnderFailure;
    }
}
