package edu.ucsc.dbtune.divgdesign;

import static edu.ucsc.dbtune.DatabaseSystem.newDatabaseSystem;
import static edu.ucsc.dbtune.util.TestUtils.workload;

import java.util.List;
import java.util.Set;

import org.junit.BeforeClass;
import org.junit.Test;

import edu.ucsc.dbtune.divgdesign.DB2DivgDesign;

import edu.ucsc.dbtune.DatabaseSystem;
import edu.ucsc.dbtune.advisor.db2.DB2Advisor;
import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.util.Environment;
import edu.ucsc.dbtune.workload.Workload;

import static edu.ucsc.dbtune.util.TestUtils.getBaseOptimizer;

public class DB2DivgDesignFunctionalTest 
{
    private static DatabaseSystem db;
    private static Environment    en;
    
    private static int nReplicas;
    private static int loadfactor;
    private static double B;
    
    private static Workload workload;
    private static DB2Advisor db2advis;
    
    /**
     * Setup for the test.
     */
    @BeforeClass
    public static void beforeClassSetUp() throws Exception
    {
        /*
        en = Environment.getInstance();
        db = newDatabaseSystem(en);
        
        workload = workload(en.getWorkloadsFoldername() + "/tpch");
        nReplicas = 3;
        loadfactor = 2;
        B = Math.pow(2, 28);
        
        // DB2Advis
        db2advis = new DB2Advisor(db);
        */
    }
    
    @Test
    public void testDivergentDesign() throws Exception
    {   
        /*
        // DivDesisgn
        double start = System.currentTimeMillis();
        DB2DivgDesign divg = new DB2DivgDesign(db2advis, getBaseOptimizer(db.getOptimizer()));
        List<Set<Index>> indexesAtReplica = divg.recommend(workload, nReplicas, loadfactor, B);
        
        System.out.println("DB2 Divergent Design \n"
                            + " Running time: " + (System.currentTimeMillis() - start) + "\n"
                            + " The objective value: " + divg.getTotalCost());
        System.out.println(" configuration: " + indexesAtReplica);
        */
    }
    
    
    @Test
    public void testUniformDesign() throws Exception
    {   
        /*
        nReplicas = 1;
        loadfactor = 1;
       
        // DivDesisgn
        double start = System.currentTimeMillis();
        DB2DivgDesign divg = new DB2DivgDesign(db2advis, getBaseOptimizer(db.getOptimizer()));
        List<Set<Index>> indexesAtReplica = divg.recommend(workload, nReplicas, loadfactor, B);
        
        System.out.println("DB2 UNIFORM Design \n"
                            + " Running time: " + (System.currentTimeMillis() - start) + "\n"
                            + " The objective value: " + divg.getTotalCost());
        System.out.println(" configuration: " + indexesAtReplica);
        */
    }
}
