package edu.ucsc.dbtune.divgdesign;


import static edu.ucsc.dbtune.util.TestUtils.getBaseOptimizer;


import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;


import org.junit.Test;

import edu.ucsc.dbtune.divgdesign.CoPhyDivgDesign;

import edu.ucsc.dbtune.advisor.candidategeneration.CandidateGenerator;
import edu.ucsc.dbtune.advisor.candidategeneration.OptimizerCandidateGenerator;
import edu.ucsc.dbtune.bip.DivTestEntry;
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
    private static List<DivTestEntry> entries;
    private static CoPhyDivgDesign divg;
    
    @Test
    public void testCoPhyDiv() throws Exception
    {   
     // 1. Read common parameter
        getEnvironmentParameters();
        
        // 2. set parameter for DivBIP()
        setParameters();
        
        if (!(io instanceof InumOptimizer))
            return;
        
        // 2. Generate candidate indexes
        generateOptimalCandidatesCoPhy();
        maxIters = 5;
        
        entries = new ArrayList<DivTestEntry>();
        String type;
        DivTestEntry entry;
        List<Double> objs = new ArrayList<Double>();
        
        
        // 3. Call CoPhy Design
        for (double B1 : listBudgets) {
            
            B = B1;
            System.out.println(" Space:  " + B + "============\n");
            type = "budget_" + B + "_" + listNumberReplicas;
            objs = new ArrayList<Double>();
            
            for (int i = 0; i < listNumberReplicas.size(); i++) {
                
                nReplicas = listNumberReplicas.get(i);
                loadfactor = (int) Math.ceil( (double) nReplicas / 2);
                
                
                System.out.println("--------------------------------------------");
                System.out.println(" DIVGDESIGN-COPHY, # replicas = " + nReplicas
                        + ", load factor = " + loadfactor
                        + ", B = " + B);
                
                testDiv();
                objs.add(divg.getTotalCost());
                System.out.println("--------------------------------------------");
            }
            
            entry = new DivTestEntry(type, objs);
            entries.add(entry);
        
        }
        // write to file
        String name = en.getWorkloadsFoldername() + "/divg_cophy.txt";
        writeDivInfoToFile(name, entries);
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
        
        
        for (SQLStatement sql : workload) {
        
            sqls = new ArrayList<SQLStatement>();
            sqls.add(sql);
            
            wl = new Workload(sqls);
            
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
