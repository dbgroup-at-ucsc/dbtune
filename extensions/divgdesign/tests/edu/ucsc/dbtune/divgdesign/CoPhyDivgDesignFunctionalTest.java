package edu.ucsc.dbtune.divgdesign;

import static edu.ucsc.dbtune.DatabaseSystem.newDatabaseSystem;
import static edu.ucsc.dbtune.util.TestUtils.workload;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.junit.BeforeClass;
import org.junit.Test;

import edu.ucsc.dbtune.divgdesign.CoPhyDivgDesign;

import edu.ucsc.dbtune.DatabaseSystem;
import edu.ucsc.dbtune.bip.div.DivConfiguration;
import edu.ucsc.dbtune.bip.div.DivergentOnOptimizer;
import edu.ucsc.dbtune.bip.util.LogListener;
import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.optimizer.InumOptimizer;
import edu.ucsc.dbtune.optimizer.Optimizer;
import edu.ucsc.dbtune.util.Environment;
import edu.ucsc.dbtune.workload.SQLStatement;
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
        
        
        int arrNReplicas[] = {2, 3, 4, 5};
        int arrLoadFactor[] = {1, 2, 2, 3};
        
        // divergent design
        for (int i = 0; i < arrNReplicas.length; i++) {
            
            nReplicas = arrNReplicas[i];
            loadfactor = arrLoadFactor[i];
            System.out.println(" DIVGDESIGN-COPHY, # replicas = " + nReplicas
                                + ", load factor = " + loadfactor);
            testDiv();
        }
    }
    
    
    private static void testDiv() throws Exception
    {
        Optimizer io = db.getOptimizer();
        LogListener logger = LogListener.getInstance();
        
        if (!(io instanceof InumOptimizer))
            return;
        
        workload = workload(en.getWorkloadsFoldername() + "/tpch");
        nReplicas = 3;
        loadfactor = 2;
        B = Math.pow(2, 28);
        
        
        CoPhyDivgDesign divg = new CoPhyDivgDesign(db, (InumOptimizer) io, logger);
        List<Set<Index>> indexesAtReplica = divg.recommend(workload, nReplicas, loadfactor, B);
        
        DivConfiguration output = new DivConfiguration(nReplicas, loadfactor);
        for (int r = 0; r < nReplicas; r++)
            for (Index index : indexesAtReplica.get(r))
                output.addIndexReplica(r, index);
        
        // run on actual optimize
        Set<SQLStatement> sqls = new HashSet<SQLStatement>();
        for (int i = 0; i < workload.size(); i++)
            sqls.add(workload.get(i));
        DivergentOnOptimizer doo = new DivergentOnOptimizer();
        doo.verify(io.getDelegate(), output, sqls);
        
        double costDB2 = doo.getTotalCost() / loadfactor;
        
        System.out.println("DB2 Divergent Design \n"
                            + " Running time: " + divg.getRunningTime() + "\n"
                            + " The objective value: " + divg.getTotalCost() + "\n"
                            + " Number of iterations: " + divg.getNumberOfIterations() + "\n"
                            + " Cost DB2: " + costDB2
                            + " ratio: " + (double) costDB2 / divg.getTotalCost());
    }
}
