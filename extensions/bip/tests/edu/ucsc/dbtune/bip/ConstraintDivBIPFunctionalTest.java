package edu.ucsc.dbtune.bip;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import edu.ucsc.dbtune.bip.core.IndexTuningOutput;
import edu.ucsc.dbtune.bip.div.ConstraintDivBIP;
import edu.ucsc.dbtune.bip.div.DivBIP;
import edu.ucsc.dbtune.bip.div.DivConstraint;
import edu.ucsc.dbtune.bip.util.LogListener;
import edu.ucsc.dbtune.optimizer.InumOptimizer;

import static edu.ucsc.dbtune.bip.div.ConstraintDivBIP.IMBALANCE_QUERY;
import static edu.ucsc.dbtune.bip.div.ConstraintDivBIP.IMBALANCE_REPLICA;
import static edu.ucsc.dbtune.bip.div.ConstraintDivBIP.UPDATE_COST_BOUND;


public class ConstraintDivBIPFunctionalTest  extends DivTestSetting
{
    private static ConstraintDivBIP constraintDiv;
    private static int nReplicaUnif;
    private static int typeConstraint;
    
    @Test
    public void testConstraintDiv() throws Exception
    {
        // 1. Set common parameters
        setCommonParameters();
        
        if (!(io instanceof InumOptimizer))
            return;

        // 2. Generate candidate indexes
        generateOptimalCandidates();
        
        typeConstraint = UPDATE_COST_BOUND;
        nReplicaUnif = 1;
        
        if (typeConstraint == UPDATE_COST_BOUND) {
            updateCostConstraints();
            return; 
        }
        
        // set of imbalance factor values
        double deltas[] = {2, 1.5, 1.05};
        //double deltas[] = {1.05};
        
        for (double delta : deltas) {
            
            System.out.println(" IMBALANCE FACTOR: " + delta + " space: "+ B + "------------" );
            
            if (typeConstraint == IMBALANCE_REPLICA)
                imbalanceReplicaConstraint(delta);
            else if (typeConstraint == IMBALANCE_QUERY)
                imbalanceQueryConstraint(delta);
            
            runConstraintBIP(constraintDiv);
        }
    }
    
    /**
     * Handle update cost constraint
     * @throws Exception
     */
    private static void updateCostConstraints() throws Exception
    {   
        div = new DivBIP();     
                
        LogListener logger = LogListener.getInstance();
        div.setCandidateIndexes(candidates);
        div.setWorkload(workload); 
        div.setOptimizer((InumOptimizer) io);
        div.setNumberReplicas(1);
        div.setLoadBalanceFactor(1);
        div.setSpaceBudget(B);
        div.setLogListenter(logger);
        
        div.solve();
        
        // get the query cost & update cost
        double queryCost;
        double updateCost;
        double boundUpdateCost;
        double totalCostUniform;
        
        // update index cost 
        updateCost = div.getUpdateCostFromCplex();
        queryCost  = div.getObjValue() - updateCost;
        // take into account the base table update cost
        updateCost += div.getTotalBaseTableUpdateCost();
        boundUpdateCost = nReplicaUnif  * updateCost;
        totalCostUniform = queryCost + updateCost * nReplicaUnif;
                
        System.out.println("DIV-UNIF, TOTAL COST: " + totalCostUniform  + "\n"
        		            + " QUERY cost: " + queryCost + "\n"
        		            + " UPDATE cost: " + updateCost + "\n"
        		            + " BOUND update cost: " + boundUpdateCost
        		            );
        
        double deltas[] = {1.5, 1, 0.5};
        //double deltas[] = {0.8};
        
        // bound update cost
        for (double delta : deltas) {
            System.out.println("=========== delta " + delta);
            boundUpdateCost(boundUpdateCost * delta);
            runConstraintBIP(constraintDiv);
        }  
    }
    
    /**
     * Initialize the object to handle imbalance replica constraints
     */
    private static void imbalanceReplicaConstraint(double delta)
    {
        DivConstraint iReplica = new DivConstraint(ConstraintDivBIP.IMBALANCE_REPLICA, delta);
        List<DivConstraint> constraints = new ArrayList<DivConstraint>();
        constraints.add(iReplica);
        
        constraintDiv = new ConstraintDivBIP(constraints, isApproximation);
    }
    
    
    /**
     * Initialize the object to handle imbalance query constraints
     */
    private static void imbalanceQueryConstraint(double delta)
    {
        DivConstraint iQuery = new DivConstraint(ConstraintDivBIP.IMBALANCE_QUERY, delta);
        List<DivConstraint> constraints = new ArrayList<DivConstraint>();
        constraints.add(iQuery);
        
        constraintDiv = new ConstraintDivBIP(constraints, isApproximation);
    }
    
    /**
     * Initialize the object to handle bound update query cost
     */
    private static void boundUpdateCost(double delta)
    {
        DivConstraint iQuery = new DivConstraint(ConstraintDivBIP.UPDATE_COST_BOUND, delta);
        List<DivConstraint> constraints = new ArrayList<DivConstraint>();
        constraints.add(iQuery);
        
        constraintDiv = new ConstraintDivBIP(constraints, isApproximation);
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
            
            updateCost += div.getTotalBaseTableUpdateCost();
            // add the update-base-table-constant costs
            double totalCostBIP = div.getObjValue() + div.getTotalBaseTableUpdateCost();
            
            System.out.println(" ------------- \n"
                    + " Number of replicas: " + nReplicas + " load factor: " + loadfactor + "\n" 
                    + " TOTAL cost: " + totalCostBIP + "\n"
                    + " QUERY cost:  " + queryCost   + "\n"
                    + " UPDATE cost: " + updateCost  + "\n"
                    + " ----- Update cost details: "  + "\n"
                    + "          + query shell & update indexes: " 
                                        + div.getUpdateCostFromCplex() + "\n"
                    + "          + update base table:             "
                                        + div.getTotalBaseTableUpdateCost() + "\n"
                    + " ----- CPLEX info: \n"
                    + "          + obj value: " + div.getObjValue() + "\n"
                    + "          + gap from the optimal: " + div.getObjectiveGap() + "\n");
                    

            // show imbalance query & replica
            System.out.println(" IMBALANCE REPLICA: " + div.getMaxImbalanceReplica());
            System.out.println(" IMBALANCE QUERY: " + div.getMaxImbalanceQuery());
            
            if (isShowRecommendation)
                System.out.println(" solution: " + output);
        } else 
            System.out.println(" NO SOLUTION ");
    }
}
