package edu.ucsc.dbtune.bip;

import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;

import org.junit.Test;


import edu.ucsc.dbtune.bip.core.IndexTuningOutput;
import edu.ucsc.dbtune.bip.div.ConstraintDivBIP;
import edu.ucsc.dbtune.bip.div.DivConstraint;
import edu.ucsc.dbtune.bip.util.LogListener;
import edu.ucsc.dbtune.optimizer.InumOptimizer;
import edu.ucsc.dbtune.util.Rt;

import static edu.ucsc.dbtune.util.EnvironmentProperties.NODE_IMBALANCE;
import static edu.ucsc.dbtune.util.EnvironmentProperties.FAILURE_IMBALANCE;

public class ConstraintDivBIPFunctionalTest  extends DivTestSetting
{
    private static ConstraintDivBIP constraintDiv;
    
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
        List<DivConstraint> constraints;
        
        // get optimal total cost value
        double optimalTotalCost = DivBIPFunctionalTest.testDiv(nReplicas, B, false);
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
        
        DivConstraint iReplica;
        
        for (int i = 0; i < nodeFactors.size(); i++){
            constraints = new ArrayList<DivConstraint>();
            iReplica = new DivConstraint(NODE_IMBALANCE, nodeFactors.get(i));
            constraints.add(iReplica);
            if (failureFactors.size() > 0){
                iReplica = new DivConstraint(FAILURE_IMBALANCE, failureFactors.get(i));
                constraints.add(iReplica);
            }
            
            Rt.p(" Number of constraints: " + constraints.size());
            constraintDiv = new ConstraintDivBIP(constraints, true, true);
            constraintDiv.setOptimalTotalCost(optimalTotalCost);
            div.clear();
            runConstraintBIP(constraintDiv);
        }
    }
    
    protected static void exactConstraintDiv() throws Exception
    {
        List<DivConstraint> constraints;
        
        /*
         double upperTotalCost;
        // get the total cost of the normal setting
        upperTotalCost = DivBIPFunctionalTest.testDiv(nReplicas, B, false);
        upperTotalCost *= 1.2;
        */
        
        
        // 2. Set constraints
        /*
        // all constraint
        if (isAllImbalanceConstraint) {
            constraints = new ArrayList<DivConstraint>();
            // separate imbalance constraint
            for (String typeConstraint : en.getListImbalanceConstraints()){
                for (double factor : en.getListImbalanceFactors()) {
                    Rt.p("\n\n\n IMBALANCE FACTOR: " + factor +                                        
                                        "space: "+ B + "------------\n \n \n" );
                    DivConstraint iReplica = new DivConstraint(typeConstraint, factor);                    
                    constraints.add(iReplica);                
                    break;
                }
            }
            
            Rt.p(" set of constraints " + constraints);
            constraintDiv = new ConstraintDivBIP(constraints, false);
            runConstraintBIP(constraintDiv);
            
            return;
        }        
        */
        
        // separate imbalance constraint
        for (String typeConstraint : en.getListImbalanceConstraints()){
            Rt.p("\n\n\n----------------" + typeConstraint
                        + " ---\n \n");
            for (double factor : en.getListImbalanceFactors()) {
                Rt.p("\n\n\n IMBALANCE FACTOR: " + factor + 
                                    " s" +
                                    "pace: "+ B + "------------\n \n \n" );
                DivConstraint iReplica = new DivConstraint(typeConstraint, factor);
                constraints = new ArrayList<DivConstraint>();
                constraints.add(iReplica);                
                constraintDiv = new ConstraintDivBIP(constraints, true, false);
                /*
                Rt.p("Check feasible solution only");
                constraintDiv.checkFeasibleSolutionOnly();
                Rt.p("set upper cost");
                constraintDiv.setUpperTotalCost(upperTotalCost);
                */
                runConstraintBIP(constraintDiv);
            }
        }
    }
    
    /**
     * Run the BIP 
     * 
     * @throws Exception
     */
    public static double runConstraintBIP(ConstraintDivBIP div) throws Exception
    {        
        double totalCostBIP = -1.0;
        io = db.getOptimizer();

        LogListener logger = LogListener.getInstance();
        div.setCandidateIndexes(candidates);
        div.setWorkload(workload); 
        div.setOptimizer((InumOptimizer) io);
        div.setNumberReplicas(nReplicas);
        div.setLoadBalanceFactor(loadfactor);
        div.setSpaceBudget(B);
        div.setLogListenter(logger);
        
        IndexTuningOutput output = div.solve();
        
        if (isExportToFile)
            div.exportCplexToFile(en.getWorkloadsFoldername() + "test.lp");
        
        Rt.p(logger.toString());
        if (output != null) {
            
            double updateCost = div.getUpdateCostFromCplex();
            double queryCost = div.getObjValue() - updateCost;
            
            //updateCost += div.getTotalBaseTableUpdateCost();
            // add the update-base-table-constant costs
            totalCostBIP = div.getObjValue(); //+ div.getTotalBaseTableUpdateCost();
            
            Rt.p(" ------------- \n"
                    + " Number of replicas: " + nReplicas + " load factor: " + loadfactor + "\n" 
                    + " TOTAL cost: " + totalCostBIP + "\n"
                    + " QUERY cost:  " + queryCost   + "\n"
                    + " UPDATE cost: " + updateCost  + "\n"
                    + " ----- Update cost details: "  + "\n"
                    + "          + query shell & update indexes: " 
                                        + div.getUpdateCostFromCplex() + "\n"
                    //+ "          + update base table:             "
                     //                   + div.getTotalBaseTableUpdateCost() + "\n"
                    + " ----- CPLEX info: \n"
                    + "          + obj value: " + div.getObjValue() + "\n"
                    + "          + gap from the optimal: " + div.getObjectiveGap() + "\n");
                    

            // show imbalance query & replica
            Rt.p(" NODE IMBALANCE: " + div.getNodeImbalance());
            Rt.p(" QUERY IMBALANCE: " + div.getQueryImbalance());
            Rt.p(" FAILURE IMBALANCE: " + div.getFailureImbalance());
            
            if (isShowRecommendation)
                Rt.p(" solution: " + output);
        } else 
            Rt.p(" NO SOLUTION ");
        
        return totalCostBIP;
    }
}
