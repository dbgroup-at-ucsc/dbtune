package edu.ucsc.dbtune.bip;

import static edu.ucsc.dbtune.DatabaseSystem.newDatabaseSystem;
import static edu.ucsc.dbtune.util.TestUtils.getBaseOptimizer;
import static edu.ucsc.dbtune.util.TestUtils.workload;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import edu.ucsc.dbtune.DatabaseSystem;
import edu.ucsc.dbtune.advisor.candidategeneration.CandidateGenerator;
import edu.ucsc.dbtune.advisor.candidategeneration.OptimizerCandidateGenerator;
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
    protected static boolean isShowRecommendation;
    protected static boolean isApproximation;
    
    protected static DivBIP div;
    
    protected int arrNReplicas[] = {2, 3, 4, 5};
    protected int arrLoadFactor[] = {1, 2, 2, 3};
    
    protected static long totalIndexSize;
    protected static Set<Index> candidates;
    
    
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
        //workload = workload(en.getWorkloadsFoldername() + "/tpch-mix-div");
        
        nReplicas  = 5;
        loadfactor = 3;
        isTestOne  = false;
        isExportToFile = false;
        isTestCost = false;
        isShowRecommendation = false;
        isApproximation = false;
        
        B   = Math.pow(2, 29);
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
    
    /**
     * Compute the query execution cost for statements in the given workload
     * 
     * @param conf
     *      A configuration
     *      
     * @throws Exception
     */
    protected static List<Double> computeQueryCostsInum(Set<Index> conf) throws Exception
    {
        InumPreparedSQLStatement inumPrepared;        
        double inumCost;
        List<Double> costs = new ArrayList<Double>();
        
        for (SQLStatement sql : workload) {
            
            inumPrepared = (InumPreparedSQLStatement) io.prepareExplain(sql);
            inumCost = inumPrepared.explain(conf).getTotalCost();
            costs.add(inumCost);
            
        }
        
        return costs;
    }
    
    /**
     * Generate candidate indexes
     */
    protected static void generateCandidates() throws Exception
    {
        CandidateGenerator candGen =
            new OptimizerCandidateGenerator(getBaseOptimizer(db.getOptimizer()));
        candidates = candGen.generate(workload);
        
        // Calculate the total size (for solely information)
        totalIndexSize = 0;
        for (Index index : candidates)
            totalIndexSize += index.getBytes();
        
        System.out.println("Number of statements: " + workload.size() + "\n"
                            + "Number of candidate: " + candidates.size() + "\n" 
                            + "Total size: " + totalIndexSize);
    }
    
}
