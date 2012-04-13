package edu.ucsc.dbtune.divgdesign;


import static edu.ucsc.dbtune.util.TestUtils.getBaseOptimizer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;


import org.junit.Test;

import com.google.caliper.internal.guava.collect.Lists;

import edu.ucsc.dbtune.divgdesign.CoPhyDivgDesign;

import edu.ucsc.dbtune.advisor.candidategeneration.CandidateGenerator;
import edu.ucsc.dbtune.advisor.candidategeneration.OptimizerCandidateGenerator;
import edu.ucsc.dbtune.bip.DivTestSetting;
import edu.ucsc.dbtune.bip.util.LogListener;
import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.optimizer.InumOptimizer;
import edu.ucsc.dbtune.workload.SQLStatement;
import edu.ucsc.dbtune.workload.Workload;


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
        
        // 2. Generate candidate indexes
        generateCandidates();
        maxIters = 5;
        
        // 3. Call CoPhy Design
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
    }
    
    
    /**
     * Generate candidate indexes for each statement
     * 
     * @throws Exception
     */
    protected void generateCandidates() throws Exception
    {
        Set<Index> candidate;
        Workload wl;
        CandidateGenerator candGen =
            new OptimizerCandidateGenerator(getBaseOptimizer(db.getOptimizer()));
        
        recommendedIndexStmt = new HashMap<SQLStatement, Set<Index>>();
        
        for (SQLStatement sql : workload) {
            
            wl = new Workload(Lists.newArrayList(sql));
            candidate = candGen.generate(wl);                
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
                            + " Running time: " + (timeInum + timeAnalysis) + "\n"
                            + " The objective value: " + divg.getTotalCost()
                            );
    }
}
