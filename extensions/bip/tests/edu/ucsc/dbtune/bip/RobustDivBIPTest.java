package edu.ucsc.dbtune.bip;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

import org.junit.Test;

import edu.ucsc.dbtune.bip.DIVPaper.DivPaperEntry;
import edu.ucsc.dbtune.bip.core.IndexTuningOutput;
import edu.ucsc.dbtune.bip.div.RobustDivBIP;
import edu.ucsc.dbtune.bip.div.UtilConstraintBuilder;
import edu.ucsc.dbtune.bip.util.LogListener;
import edu.ucsc.dbtune.optimizer.InumOptimizer;
import edu.ucsc.dbtune.util.Rt;

import static edu.ucsc.dbtune.bip.div.UtilConstraintBuilder.computeCoefCostWithoutFailure;
import static edu.ucsc.dbtune.bip.div.UtilConstraintBuilder.computeCoefCostWithFailure;

public class RobustDivBIPTest extends DIVPaper 
{   
    private static RobustDivBIP constraintDiv;
    
 
    @Test
    public void testConstraintDiv() throws Exception
    {
        // 1. Set common parameters
        initialize();
        getEnvironmentParameters();
        setParameters();
        
        if (!(io instanceof InumOptimizer))
            return;
        
        // test with greedy first
        //testConstraintDiv(true);
        testConstraintDiv(true);
        
        // not to draw graph
        resetParameterNotDrawingGraph();
    }
    
    /**
     * Use greedy approach
     * 
     * @throws Exception
     */
    protected static void testConstraintDiv(boolean isGreedy) throws Exception
    {   
        double optimalTotalCost;
        
        // get optimal total cost value
        if (isGreedy) {
            optimalTotalCost = DivBIPFunctionalTest.testDiv(nReplicas, B, false);
            div.clear();
        } else
            optimalTotalCost = -1;

        Rt.p(" optimal total cost = " + optimalTotalCost);
        List<Double> nodeFactors = new ArrayList<Double>();
        List<Double> failureFactors = new ArrayList<Double>();
        
        // get the imbalance factors from inputs
        try {   
            nodeFactors = en.getNodeImbalanceFactors();
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
        
        for (int i = 0; i < nodeFactors.size(); i++){
            // initialize object
            constraintDiv = new RobustDivBIP(optimalTotalCost, nodeFactors.get(i), 
                             failureFactors.get(i));
            runImbalanceConstraintBIP(constraintDiv, isGreedy);
        }
    }
    
    /**
     * Run the BIP 
     * 
     * @throws Exception
     */
    public static double runImbalanceConstraintBIP(RobustDivBIP robustDiv, boolean isGreedy) 
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
        
        if (isExportToFile) 
            robustDiv.exportCplexToFile(en.getWorkloadsFoldername() + "/test.lp");
        
        Rt.p(logger.toString());
        if (output != null) {
            
            double costUnifUnderFailure = computeCostUNIFUnderFailure (constraintDiv.getFailureFactor(), 
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
            
            
            if (isShowRecommendation)
                Rt.p(" solution: " + output);
            
            // show imbalance query & replica
            Rt.p(" NODE IMBALANCE: " + robustDiv.getNodeImbalance());
            
        } else 
            Rt.p(" NO SOLUTION ");
        
        return totalCostBIP;
    }
    
    protected static double computeCostUNIFUnderFailure(double factor, int N)
            throws Exception
    {
        double costUnifUnderFailure = 1.0;
        double costUnif;
        //  1. Read the result from UNIF file
        unifFile = new File(rawDataDir, wlName + "_" + UNIF_DB2_FILE);
        mapUnif = readDivResult(unifFile);
    
        DivPaperEntry entry = new DivPaperEntry(dbName, wlName, nReplicas, convertBudgetToMB (B));
        if (!mapUnif.containsKey(entry)) {
            entry = new DivPaperEntry(dbName.toLowerCase(), wlName, nReplicas, convertBudgetToMB (B));
        }
        
        costUnif = mapUnif.get(entry);
        costUnifUnderFailure = 
            (computeCoefCostWithoutFailure(factor, N)
                    + computeCoefCostWithFailure(factor, N) / N)
                    * costUnif;
        Rt.p(" COST UNIF = " + costUnif
                + " COST UNIF UNDER FAILURE = " + costUnifUnderFailure);
        
        return costUnifUnderFailure;
    }
}
