package edu.ucsc.dbtune.bip;

import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;

import org.junit.Test;

import edu.ucsc.dbtune.bip.core.IndexTuningOutput;
import edu.ucsc.dbtune.bip.div.RobustDivBIP;
import edu.ucsc.dbtune.bip.util.LogListener;
import edu.ucsc.dbtune.optimizer.InumOptimizer;
import edu.ucsc.dbtune.util.Rt;

public class RobustDivBIPTest extends DivTestSetting 
{   
    private static RobustDivBIP constraintDiv;
 
    @Test
    public void testConstraintDiv() throws Exception
    {
        // 1. Set common parameters
        getEnvironmentParameters();
        setParameters();
        
        if (!(io instanceof InumOptimizer))
            return;
        
        greedyConstraintDiv();
    }
    
    /**
     * Use greedy approach
     * 
     * @throws Exception
     */
    protected static void greedyConstraintDiv() throws Exception
    {   
        // get optimal total cost value
        double optimalTotalCost = DivBIPFunctionalTest.testDiv(nReplicas, B, false);
        div.clear();
        
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
            runConstraintBIP(constraintDiv);
        }
    }
    
    /**
     * Run the BIP 
     * 
     * @throws Exception
     */
    public static double runConstraintBIP(RobustDivBIP robustDiv) throws Exception
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
        
        if (isExportToFile) {
            Rt.p("export file here");
            robustDiv.exportCplexToFile(en.getWorkloadsFoldername() + "test.lp");
        }
        
        Rt.p(logger.toString());
        if (output != null) {
            totalCostBIP = robustDiv.getTotalCost(); 
            
            Rt.p(" ------------- \n"
                    + " Number of replicas: " + nReplicas + " load factor: " + loadfactor + "\n" 
                    + " TOTAL cost: " + totalCostBIP + "\n"                    + " ----- Update cost details: "  + "\n"
                    + "          + query shell & update indexes: " 
                                        + robustDiv.getUpdateCostFromCplex() + "\n"
                    + " ----- CPLEX info: \n"
                    + "          + obj value: " + robustDiv.getObjValue() + "\n"
                    + "          + gap from the optimal: " + robustDiv.getObjectiveGap() + "\n");
            Rt.p(" COMPARE WITH UNIF: " + robustDiv.getObjValue()/robustDiv.getCoefTotalCost());
            
            if (isShowRecommendation)
                Rt.p(" solution: " + output);
            
            // show imbalance query & replica
            Rt.p(" NODE IMBALANCE: " + robustDiv.getNodeImbalance());
            
            
        } else 
            Rt.p(" NO SOLUTION ");
        
        return totalCostBIP;
    }
}
