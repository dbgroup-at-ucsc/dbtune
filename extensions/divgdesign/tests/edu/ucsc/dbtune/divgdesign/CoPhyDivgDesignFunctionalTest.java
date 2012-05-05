package edu.ucsc.dbtune.divgdesign;


import static edu.ucsc.dbtune.util.OptimizerUtils.getBaseOptimizer;
import static edu.ucsc.dbtune.util.TestUtils.workload;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;


import org.junit.Test;

import com.google.caliper.internal.guava.collect.Lists;

import edu.ucsc.dbtune.divgdesign.CoPhyDivgDesign;

import edu.ucsc.dbtune.advisor.candidategeneration.CandidateGenerator;
import edu.ucsc.dbtune.advisor.candidategeneration.OptimizerCandidateGenerator;
import edu.ucsc.dbtune.advisor.candidategeneration.PowerSetOptimalCandidateGenerator;
import edu.ucsc.dbtune.bip.DivTestSetting;
import edu.ucsc.dbtune.bip.util.LogListener;
import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.optimizer.InumOptimizer;
import edu.ucsc.dbtune.workload.SQLStatement;
import edu.ucsc.dbtune.workload.Workload;

import static edu.ucsc.dbtune.workload.SQLCategory.SELECT;


/**
 * Test the usage of CoPhy in DivgDesign
 * 
 * @author Quoc Trung Tran
 *
 */
public class CoPhyDivgDesignFunctionalTest extends DivTestSetting
{   
    private static int maxIters;
    private static Map<SQLStatement, Set<Index>> recommendedIndexStmt;
    
    
    @Test
    public void testCoPhyDiv() throws Exception
    {   
        // 1. Set common parameters
        setCommonParameters();

        if (!(io instanceof InumOptimizer))
            return;
        
        // 2. Generate candidate indexes
        generateOptimalCandidatesCoPhy();
        maxIters = 1;
        
        // 3. Call CoPhy Design
        for (double B1 : lB) {
            
            B = B1;
            System.out.println(" Space:  " + B + "============\n");
            
            for (int i = 0; i < arrNReplicas.length; i++) {
                
                nReplicas = arrNReplicas[i];
                loadfactor = arrLoadFactor[i];
                
                if (isTestOne && nReplicas != 4)
                    continue;
                
                System.out.println("--------------------------------------------");
                System.out.println(" DIVGDESIGN-COPHY, # replicas = " + nReplicas
                        + ", load factor = " + loadfactor
                        + ", B = " + B);
                
                testDiv();
                System.out.println("--------------------------------------------");
            }
            
            if (isOneBudget)
                break;
        }
    }
    
    
    /**
     * Generate optimal candidate indexes for each statement
     * 
     * @throws Exception
     */
    protected void generateOptimalCandidatesCoPhy() throws Exception
    {
        Set<Index> candidate;
        Workload wl;
        CandidateGenerator candGen =
            new OptimizerCandidateGenerator(getBaseOptimizer(db.getOptimizer()));
        
        recommendedIndexStmt = new HashMap<SQLStatement, Set<Index>>();
        List<SQLStatement> sqls;
        //Workload wlExtra = workload(en.getWorkloadsFoldername() + "/tpch-extra");
        Workload wlExtra = workload(en.getWorkloadsFoldername() + "/tpch-10-counts");
        
        for (SQLStatement sql : workload) {
        
            sqls = new ArrayList<SQLStatement>();
            sqls.add(sql);
            
            // extra workload
            if (isExtraWorkload) {
                for (SQLStatement e : wlExtra)
                    if (e.getSQLCategory().isSame(SELECT))
                        sqls.add(e);
                
            }
            
            wl = new Workload(sqls);
            
            if (sql.getSQLCategory().isSame(SELECT))
                candidate = candGen.generate(wl);
            else 
                candidate = new HashSet<Index>();
            
            recommendedIndexStmt.put(sql, candidate);
            
        }
    }
    
    /**
     * Generate powerset candidate indexes for each statement
     * 
     * @throws Exception
     */
    protected void generatePowersetCandidatesCoPhy() throws Exception
    {
        Set<Index> candidate;
        Workload wl;
        CandidateGenerator candGen = 
            new PowerSetOptimalCandidateGenerator(
                    new OptimizerCandidateGenerator
                        (getBaseOptimizer(db.getOptimizer())), 2);
        
        recommendedIndexStmt = new HashMap<SQLStatement, Set<Index>>();
        
        for (SQLStatement sql : workload) {
            
            wl = new Workload(Lists.newArrayList(sql));
            
            if (sql.getSQLCategory().isSame(SELECT))
                candidate = candGen.generate(wl);
            else 
                candidate = new HashSet<Index>();
            
            recommendedIndexStmt.put(sql, candidate);
            
        }
    }
    
    /**
     * Run the CoPhyDiv algorithm.
     * 
     * @throws Exception
     */
    private static void testDiv() throws Exception
    {
        LogListener logger; 
        List<CoPhyDivgDesign> divgs = new ArrayList<CoPhyDivgDesign>();
        
        // run at most {@code maxIters} times
        int minPosition = -1;
        double minCost = -1;
        CoPhyDivgDesign divg;
        
        for (int iter = 0; iter < maxIters; iter++) {
            
            logger = LogListener.getInstance();
            divg = new CoPhyDivgDesign(db, (InumOptimizer) io, logger, recommendedIndexStmt);
            divg.recommend(workload, nReplicas, loadfactor, B);
            
            divgs.add(divg);
            
            if (iter == 0 || minCost > divg.getTotalCost()) {
                minPosition = iter;
                minCost = divg.getTotalCost();
            } 
        }
        
        // get the best among these runs
        double timeAnalysis = 0.0;
        double timeInum = 0.0;
        for (CoPhyDivgDesign div : divgs) {
            timeInum += div.getInumTime();
            timeAnalysis += div.getAnalysisTime();
            System.out.println("cost: " + div.getTotalCost()
                            + " Number of iterations: " + div.getNumberOfIterations()
                            + " INUM time : " + div.getInumTime()
                            + " Analysis time: " + div.getAnalysisTime());
            
        }
        
        System.out.println(" min iteration: " + minPosition + " cost: " + minCost);
        divg = divgs.get(minPosition);
        
        System.out.println("CoPhy Divergent Design \n"
                            + " INUM time: " + timeInum + "\n"
                            + " ANALYSIS time: " + timeAnalysis + "\n"
                            + " TOTAL running time: " + (timeInum + timeAnalysis) + "\n"
                            + " The objective value: " + divg.getTotalCost() + "\n"
                            + "      QUERY cost:    " + divg.getQueryCost()  + "\n"
                            + "      UPDATE cost:   " + divg.getUpdateCost() + "\n"
                            );
    }
}
