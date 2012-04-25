package edu.ucsc.dbtune.bip;

import static edu.ucsc.dbtune.DatabaseSystem.newDatabaseSystem;
import static edu.ucsc.dbtune.util.TestUtils.getBaseOptimizer;
import static edu.ucsc.dbtune.util.TestUtils.workload;

import java.util.Set;

import org.junit.BeforeClass;
import org.junit.Test;

import edu.ucsc.dbtune.DatabaseSystem;
import edu.ucsc.dbtune.advisor.candidategeneration.CandidateGenerator;
import edu.ucsc.dbtune.advisor.candidategeneration.OptimizerCandidateGenerator;
import edu.ucsc.dbtune.bip.div.DivBIP;
import edu.ucsc.dbtune.bip.div.DivConfiguration;
import edu.ucsc.dbtune.bip.div.ElasticDivBIP;
import edu.ucsc.dbtune.bip.util.LogListener;
import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.optimizer.InumOptimizer;
import edu.ucsc.dbtune.optimizer.Optimizer;
import edu.ucsc.dbtune.util.Environment;
import edu.ucsc.dbtune.workload.Workload;

public class ElasticDivBIPFunctionalTest 
{
    private static DatabaseSystem db;
    private static Environment    en;
    
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
    public void testShrinkReplicaDivergentDesign() throws Exception
    {   

        if (!(db.getOptimizer() instanceof InumOptimizer))
            return;
        /*
        int Nreplicas = 4;
        int loadfactor = 2;
        double B;
        
        DivBIP div = new DivBIP();
        
        Workload workload = workload(en.getWorkloadsFoldername() + "/tpch-inum");
        CandidateGenerator candGen = 
            new OptimizerCandidateGenerator(getBaseOptimizer(db.getOptimizer()));
        Set<Index> candidates = candGen.generate(workload);
       
        for (Index index : candidates)
            index.setBytes(1);
        System.out.println("L62, number of candidate: " + candidates.size());
        
        // At most three indexes are deployed at one replica
        B = 3;
        
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
        
        DivConfiguration initial = (DivConfiguration) div.solve();
        
        System.out.println("L77, finish DivBIP");
        // Call elastic
        ElasticDivBIP elastic = new ElasticDivBIP();
        elastic.setCandidateIndexes(candidates);
        elastic.setWorkload(workload); 
        elastic.setOptimizer((InumOptimizer) io);
        elastic.setSpaceBudget(B);
        elastic.setLogListenter(logger);
        
        elastic.setUpperDeployCost(0.0);
        elastic.setInitialConfiguration(initial);
        elastic.setNumberDeployReplicas(2);
        
        DivConfiguration after = (DivConfiguration) elastic.solve();
        
        // output the result
        System.out.println(logger);
        System.out.println("before: " + initial);
        System.out.println("after: " + after);
        */
    }
}
