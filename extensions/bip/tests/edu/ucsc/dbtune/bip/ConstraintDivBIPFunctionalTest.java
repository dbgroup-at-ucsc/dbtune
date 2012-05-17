package edu.ucsc.dbtune.bip;

import static edu.ucsc.dbtune.bip.CandidateGeneratorFunctionalTest.readCandidateIndexes;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import edu.ucsc.dbtune.bip.core.IndexTuningOutput;
import edu.ucsc.dbtune.bip.div.ConstraintDivBIP;
import edu.ucsc.dbtune.bip.div.DivConstraint;
import edu.ucsc.dbtune.bip.util.LogListener;
import edu.ucsc.dbtune.optimizer.InumOptimizer;


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

        candidates = readCandidateIndexes();
        
        
        // 2. Set constraints    
        List<DivConstraint> constraints;
        
        // all constraint
        if (isAllImbalanceConstraint) {
            
            constraints = new ArrayList<DivConstraint>();
            
            // separate imbalance constraint
            for (String typeConstraint : en.getListImbalanceConstraints()){                
                
                for (double factor : en.getListImbalanceFactors()) {
                    
                    System.out.println("\n\n\n IMBALANCE FACTOR: " + factor +                                        
                                        "space: "+ B + "------------\n \n \n" );
                    
                    DivConstraint iReplica = new DivConstraint(typeConstraint, factor);                    
                    constraints.add(iReplica);                
                    break;
                }
            }
            
            System.out.println(" set of constraints " + constraints);
            constraintDiv = new ConstraintDivBIP(constraints, false);                
            runConstraintBIP(constraintDiv);
            
            return;
        }
        
        // separate imbalance constraint
        for (String typeConstraint : en.getListImbalanceConstraints()){
            
            System.out.println("\n\n\n----------------" + typeConstraint
                        + " ---\n \n");
            
            for (double factor : en.getListImbalanceFactors()) {
                
                System.out.println("\n\n\n IMBALANCE FACTOR: " + factor + 
                                    " s" +
                                    "pace: "+ B + "------------\n \n \n" );
                
                DivConstraint iReplica = new DivConstraint(typeConstraint, factor);
                constraints = new ArrayList<DivConstraint>();
                constraints.add(iReplica);                
                constraintDiv = new ConstraintDivBIP(constraints, false);                
                runConstraintBIP(constraintDiv);
            }
        }
    }
    
    
    /**
     * Run the BIP 
     * 
     * @throws Exception
     */
    private static void runConstraintBIP(ConstraintDivBIP div) throws Exception
    {           
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
        
        
        System.out.println(logger.toString());
        if (output != null) {
            
            double updateCost = div.getUpdateCostFromCplex();
            double queryCost = div.getObjValue() - updateCost;
            
            //updateCost += div.getTotalBaseTableUpdateCost();
            // add the update-base-table-constant costs
            double totalCostBIP = div.getObjValue(); //+ div.getTotalBaseTableUpdateCost();
            
            System.out.println(" ------------- \n"
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
            System.out.println(" IMBALANCE REPLICA: " + div.getMaxImbalanceReplica());
            System.out.println(" IMBALANCE QUERY: " + div.getMaxImbalanceQuery());
            System.out.println(" NODE FAILURE: " + div.getMaxNodeFailure());
            
            if (isShowRecommendation)
                System.out.println(" solution: " + output);
        } else 
            System.out.println(" NO SOLUTION ");
    }
}
