package edu.ucsc.dbtune.bip;

import static edu.ucsc.dbtune.DatabaseSystem.newDatabaseSystem;
import static edu.ucsc.dbtune.util.TestUtils.getBaseOptimizer;
import static edu.ucsc.dbtune.util.TestUtils.workload;


import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.junit.BeforeClass;
import org.junit.Test;

import edu.ucsc.dbtune.DatabaseSystem;
import edu.ucsc.dbtune.advisor.candidategeneration.CandidateGenerator;
import edu.ucsc.dbtune.advisor.candidategeneration.OptimizerCandidateGenerator;
import edu.ucsc.dbtune.bip.core.IndexTuningOutput;
import edu.ucsc.dbtune.bip.div.ConstraintDivBIP;
import edu.ucsc.dbtune.bip.div.DivConstraint;
import edu.ucsc.dbtune.bip.util.LogListener;
import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.optimizer.InumOptimizer;
import edu.ucsc.dbtune.optimizer.Optimizer;
import edu.ucsc.dbtune.util.Environment;
import edu.ucsc.dbtune.workload.Workload;

public class ConstraintDivBIPFunctionalTest extends BIPTestConfiguration 
{
    private static DatabaseSystem db;
    private static Environment    en;
    
    private static int Nreplicas;
    private static int loadfactor;
    private static double B;
    private static ConstraintDivBIP div;
    /**
     * Setup for the test.
     */
    @BeforeClass
    public static void beforeClassSetUp() throws Exception
    {
        en = Environment.getInstance();
        db = newDatabaseSystem(en);
    }
    
    @Test
    public void testDivergentDesign() throws Exception
    {  
        // parameter setting for divergent design tuning
        Nreplicas = 4;
        loadfactor = 2;
        B = Math.pow(2, 28);
        
        imbalanceReplicaConstraint();
        runConstraintBIP();
    }
    
    /**
     * Initialize the object to handle imbalance replica constraints
     */
    private static void imbalanceReplicaConstraint()
    {
        DivConstraint iReplica = new DivConstraint(ConstraintDivBIP.IMBALANCE_REPLICA, 1.5);
        List<DivConstraint> constraints = new ArrayList<DivConstraint>();
        constraints.add(iReplica);
        
        div = new ConstraintDivBIP(constraints);
    }
    
    
    /**
     * Initialize the object to handle imbalance query constraints
     */
    private static void imbalanceQueryConstraint()
    {
        DivConstraint iQuery = new DivConstraint(ConstraintDivBIP.IMBALANCE_QUERY, 1.5);
        List<DivConstraint> constraints = new ArrayList<DivConstraint>();
        constraints.add(iQuery);
        
        div = new ConstraintDivBIP(constraints);
    }
    
    /**
     * Initialize the object to handle bound update query cost
     */
    private static void boundUdateCost()
    {
        DivConstraint iQuery = new DivConstraint(ConstraintDivBIP.UPDATE_COST_BOUND, 1.5);
        List<DivConstraint> constraints = new ArrayList<DivConstraint>();
        constraints.add(iQuery);
        
        div = new ConstraintDivBIP(constraints);
    }
    
    /**
     * Run the BIP 
     * 
     * @throws Exception
     */
    private static void runConstraintBIP() throws Exception
    {
        Workload workload = workload(en.getWorkloadsFoldername() + "/tpch");
        CandidateGenerator candGen = 
            new OptimizerCandidateGenerator(getBaseOptimizer(db.getOptimizer()));
        Set<Index> candidates = candGen.generate(workload);
        
        System.out.println("L75, number of candidate: " + candidates.size());
                
        Optimizer io = db.getOptimizer();

        if (!(io instanceof InumOptimizer))
            throw new Exception("Expecting InumOptimizer instance");
                
        LogListener logger = LogListener.getInstance();
        div.setCandidateIndexes(candidates);
        div.setWorkload(workload); 
        div.setOptimizer((InumOptimizer) io);
        div.setNumberReplicas(Nreplicas);
        div.setLoadBalanceFactor(loadfactor);
        div.setSpaceBudget(B);
        div.setLogListenter(logger);
        
        IndexTuningOutput output = div.solve();
        div.exportCplexToFile(en.getWorkloadsFoldername() + "/tpch/test.lp");
        System.out.println(logger.toString());
        if (output != null) {
            System.out.println("In test, result: " 
                              + " obj value: " + div.getObjValue()
                              + " different from optimal value: " + div.getObjectiveGap());
            div.costFromCplex(); 
        } else 
            System.out.println(" NO SOLUTION ");
    }
    
    
}
