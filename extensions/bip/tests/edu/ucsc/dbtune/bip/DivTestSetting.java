package edu.ucsc.dbtune.bip;

import static edu.ucsc.dbtune.DatabaseSystem.newDatabaseSystem;
import static edu.ucsc.dbtune.util.TestUtils.workload;

import java.util.Set;

import edu.ucsc.dbtune.DatabaseSystem;
import edu.ucsc.dbtune.bip.div.DivBIP;
import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.optimizer.InumOptimizer;
import edu.ucsc.dbtune.optimizer.InumPreparedSQLStatement;
import edu.ucsc.dbtune.optimizer.Optimizer;
import edu.ucsc.dbtune.util.Environment;
import edu.ucsc.dbtune.workload.SQLStatement;
import edu.ucsc.dbtune.workload.Workload;

/**
 * The common setting parameters for DIVBIP test
 * 
 * @author Quoc Trung Tran
 *
 */
public class DivTestSetting 
{
    protected static DatabaseSystem db;
    protected static Environment    en;
    protected static Optimizer io;
    protected static Workload workload;
    
    protected static int nReplicas;
    protected static int loadfactor;
    protected static double B;    
    
    protected static boolean isTestOne;
    protected static boolean isExportToFile;
    protected static boolean isTestCost;
    
    protected static DivBIP div;
    
    protected int arrNReplicas[] = {2, 3, 4, 5};
    protected int arrLoadFactor[] = {1, 2, 2, 3};
    
    
    /**
     * Set common parameter (e.g., workload, number of replicas, etc. )
     * 
     * @throws Exception
     */
    protected static void setCommonParameters() throws Exception
    {
        en = Environment.getInstance();
        db = newDatabaseSystem(en);        
        io = db.getOptimizer();
        
        if (!(io instanceof InumOptimizer))
            return;

        workload = workload(en.getWorkloadsFoldername() + "/tpch-small");
        
        nReplicas  = 4;
        loadfactor = 2;
        isTestOne  = true;
        isExportToFile = true;
        isTestCost = true;
        
        B   = Math.pow(2, 28);
        div = new DivBIP();
    }
    
    /**
     * Compute the query execution cost for statements in the given workload
     * 
     * @param conf
     *      A configuration
     *      
     * @throws Exception
     */
    protected static void computeQueryCosts(Set<Index> conf) throws Exception
    {
        InumPreparedSQLStatement inumPrepared;
        double db2cost;
        double inumcost;
        
        System.out.println("==============================================");
        System.out.println("Candidate: " + conf.size());
        for (Index index : conf)
            System.out.print(index.getId() + " ");
        System.out.println("\n DB2   INUM   DB2/ INUM");
        
        for (SQLStatement sql : workload) {
            
            inumPrepared = (InumPreparedSQLStatement) io.prepareExplain(sql);
            inumcost = inumPrepared.explain(conf).getTotalCost();
            db2cost = io.getDelegate().explain(sql, conf).getTotalCost();
            
            System.out.println(db2cost + " " + inumcost + " " 
                                + (double) db2cost / inumcost);
        }
    }
}
