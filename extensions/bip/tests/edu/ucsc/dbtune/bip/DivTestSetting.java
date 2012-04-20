package edu.ucsc.dbtune.bip;

import static edu.ucsc.dbtune.DatabaseSystem.newDatabaseSystem;
import static edu.ucsc.dbtune.util.TestUtils.getBaseOptimizer;
import static edu.ucsc.dbtune.util.TestUtils.workload;
import static edu.ucsc.dbtune.workload.SQLCategory.SELECT;

import static edu.ucsc.dbtune.workload.SQLCategory.INSERT;
import static edu.ucsc.dbtune.workload.SQLCategory.DELETE;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import edu.ucsc.dbtune.DatabaseSystem;
import edu.ucsc.dbtune.advisor.candidategeneration.CandidateGenerator;
import edu.ucsc.dbtune.advisor.candidategeneration.OptimizerCandidateGenerator;
import edu.ucsc.dbtune.advisor.candidategeneration.PowerSetOptimalCandidateGenerator;
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
    protected static boolean isCandidatePowerset;
    protected static double fQuery;
    protected static boolean isExtraWorkload;
    
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
        
        
        
        //workload = workload(en.getWorkloadsFoldername() + "/tpch-inum");
        workload = workload(en.getWorkloadsFoldername() + "/tpch-benchmark-mix");
        //workload = workload(en.getWorkloadsFoldername() + "/tpch-mix-div");
        
        // TODO: issue #210, extract the weight of statements
        // temporary set the INSERT/DELETE with weight = 100
        fQuery = 5.0;
        for (SQLStatement sql : workload)
            if (sql.getSQLCategory().isSame(INSERT))
                sql.setStatementWeight(3500);
            else if (sql.getSQLCategory().isSame(DELETE))
                sql.setStatementWeight(1500);
            else 
                sql.setStatementWeight(fQuery);
        
        nReplicas  = 4;
        loadfactor = 2;
        isTestOne  = false;
        isExportToFile = false;
        isTestCost = false;
        isShowRecommendation = false;
        isApproximation = false;
        isCandidatePowerset = false;
        isExtraWorkload = true;
        
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
        
        for (SQLStatement sql : workload) 
        {
            
            inumPrepared = (InumPreparedSQLStatement) io.prepareExplain(sql);
            inumCost = inumPrepared.explain(conf).getTotalCost();
            costs.add(inumCost);
            
        }
        
        return costs;
    }
    
    /**
     * Generate optimal candidate indexes
     */
    protected static void generateOptimalCandidates() throws Exception
    {
        CandidateGenerator candGen =
            new OptimizerCandidateGenerator(getBaseOptimizer(db.getOptimizer()));
        
        // temporary get only SELECT statements
        List<SQLStatement> sqls = new ArrayList<SQLStatement>();
        
        for (SQLStatement sql : workload)
            if (sql.getSQLCategory().isSame(SELECT))
                sqls.add(sql);
        
        // extra workload
        if (isExtraWorkload) {
            Workload wlExtra = workload(en.getWorkloadsFoldername() + "/tpch-extra");
            
            for (SQLStatement sql : wlExtra)
                if (sql.getSQLCategory().isSame(SELECT))
                    sqls.add(sql);
            
        }
        
        Workload wlCandidate = new Workload(sqls);
        candidates = candGen.generate(wlCandidate);
        
        // Calculate the total size (for solely information)
        totalIndexSize = 0;
        for (Index index : candidates)
            totalIndexSize += index.getBytes();
        
        System.out.println("Number of statements to generate candidate: " + wlCandidate.size() + "\n"
                            + "Number of candidate: " + candidates.size() + "\n" 
                            + "Total size: " + totalIndexSize);
    }
    
    
    /**
     * Generate powerset candidate indexes
     */
    protected static void generatePowersetCandidates() throws Exception
    {
        CandidateGenerator candGen = 
                        new PowerSetOptimalCandidateGenerator(
                                new OptimizerCandidateGenerator
                                    (getBaseOptimizer(db.getOptimizer())), 2);
        
        // temporary get only SELECT statements
        List<SQLStatement> sqls = new ArrayList<SQLStatement>();
        
        for (SQLStatement sql : workload)
            if (sql.getSQLCategory().isSame(SELECT))
                sqls.add(sql);
        
        Workload wlCandidate = new Workload(sqls);
        candidates = candGen.generate(wlCandidate);
        
        // Calculate the total size (for solely information)
        totalIndexSize = 0;
        for (Index index : candidates) {
            totalIndexSize += index.getBytes();
            System.out.println(" index " + index + " size = " + index.getBytes());
        }
        
        System.out.println("Number of statements: " + workload.size() + "\n"
                            + "Number of candidate: " + candidates.size() + "\n" 
                            + "Total size: " + totalIndexSize);
    }
    
}
