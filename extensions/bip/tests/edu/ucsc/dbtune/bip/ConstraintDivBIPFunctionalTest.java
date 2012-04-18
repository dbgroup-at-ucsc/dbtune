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


public class ConstraintDivBIPFunctionalTest  extends DivBIPFunctionalTest
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
        generateCandidates();
        
        typeConstraint = UPDATE_COST_BOUND;
        nReplicaUnif = nReplicas - 1;
        
        if (typeConstraint == UPDATE_COST_BOUND) {
            updateCostConstraints();
            return; 
        }
        
        // set of imbalance factor values
        double deltas[] = {2, 1.5, 1.05};
        
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
        updateCost = div.getUpdateCost();
        queryCost  = div.getObjValue() - updateCost;
        // take into account the base table update cost
        updateCost += div.getTotalBaseTableUpdateCost();
        boundUpdateCost = nReplicaUnif  * updateCost * 1.4;
        totalCostUniform = queryCost + updateCost * nReplicaUnif;
                
        System.out.println("L142, DIV-UNIF, query cost: " + queryCost + "\n"
                            + " update cost: " + updateCost + "\n"
                            + " update base table cost: " + div.getTotalBaseTableUpdateCost() + "\n"
                            + " bound cost: " + boundUpdateCost + "\n"
                            + " TOTAL COST: " + totalCostUniform);
        
        // bound update cost
        boundUpdateCost(boundUpdateCost);
        runConstraintBIP(constraintDiv);
    }
    
    /**
     * Initialize the object to handle imbalance replica constraints
     */
    private static void imbalanceReplicaConstraint(double delta)
    {
        DivConstraint iReplica = new DivConstraint(ConstraintDivBIP.IMBALANCE_REPLICA, delta);
        List<DivConstraint> constraints = new ArrayList<DivConstraint>();
        constraints.add(iReplica);
        
        constraintDiv = new ConstraintDivBIP(constraints);
    }
    
    
    /**
     * Initialize the object to handle imbalance query constraints
     */
    private static void imbalanceQueryConstraint(double delta)
    {
        DivConstraint iQuery = new DivConstraint(ConstraintDivBIP.IMBALANCE_QUERY, delta);
        List<DivConstraint> constraints = new ArrayList<DivConstraint>();
        constraints.add(iQuery);
        
        constraintDiv = new ConstraintDivBIP(constraints);
    }
    
    /**
     * Initialize the object to handle bound update query cost
     */
    private static void boundUpdateCost(double delta)
    {
        DivConstraint iQuery = new DivConstraint(ConstraintDivBIP.UPDATE_COST_BOUND, delta);
        List<DivConstraint> constraints = new ArrayList<DivConstraint>();
        constraints.add(iQuery);
        
        constraintDiv = new ConstraintDivBIP(constraints);
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
            System.out.println("The obj value: " + div.getObjValue() + "\n"
                               + " Different from optimal value: " + div.getObjectiveGap());
        } else 
            System.out.println(" NO SOLUTION ");
    }
}
