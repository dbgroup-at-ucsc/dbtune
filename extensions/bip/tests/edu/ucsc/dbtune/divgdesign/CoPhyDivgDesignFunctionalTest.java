package edu.ucsc.dbtune.divgdesign;

import static edu.ucsc.dbtune.DatabaseSystem.newDatabaseSystem;
import static edu.ucsc.dbtune.util.TestUtils.workload;

import java.util.List;
import java.util.Set;

import org.junit.BeforeClass;
import org.junit.Test;

import edu.ucsc.dbtune.DatabaseSystem;
import edu.ucsc.dbtune.bip.util.LogListener;
import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.optimizer.InumOptimizer;
import edu.ucsc.dbtune.optimizer.Optimizer;
import edu.ucsc.dbtune.util.Environment;
import edu.ucsc.dbtune.workload.Workload;

public class CoPhyDivgDesignFunctionalTest 
{
    private static DatabaseSystem db;
    private static Environment    en;
    
    private static int nReplicas;
    private static int loadfactor;
    private static double B;
    
    private static Workload workload;
    
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
        workload = workload(en.getWorkloadsFoldername() + "/tpch");
        nReplicas = 3;
        loadfactor = 2;
        B = Math.pow(2, 28);
        
        Optimizer io = db.getOptimizer();
        LogListener logger = LogListener.getInstance();
        
        if (!(io instanceof InumOptimizer))
            throw new Exception("Expecting InumOptimizer instance");
        
        // DivDesisgn
        double start = System.currentTimeMillis();
        CoPhyDivgDesign divg = new CoPhyDivgDesign(db, (InumOptimizer) io, logger);
        List<Set<Index>> indexesAtReplica = divg.recommend(workload, nReplicas, loadfactor, B);
        
        System.out.println("DB2 Divergent Design \n"
                            + " Running time: " + (System.currentTimeMillis() - start) + "\n"
                            + " The objective value: " + divg.getTotalCost() + "\n"
                            + " Number of iterations: " + divg.getNumberOfIterations());
        System.out.println(" configuration: " + indexesAtReplica);
    }
}
