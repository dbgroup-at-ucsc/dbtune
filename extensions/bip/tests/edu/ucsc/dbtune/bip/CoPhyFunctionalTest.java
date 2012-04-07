package edu.ucsc.dbtune.bip;

import static edu.ucsc.dbtune.DatabaseSystem.newDatabaseSystem;
import static edu.ucsc.dbtune.util.TestUtils.getBaseOptimizer;
import static edu.ucsc.dbtune.util.TestUtils.workload;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.junit.BeforeClass;
import org.junit.Test;

import edu.ucsc.dbtune.DatabaseSystem;
import edu.ucsc.dbtune.advisor.candidategeneration.CandidateGenerator;
import edu.ucsc.dbtune.advisor.candidategeneration.OptimizerCandidateGenerator;
import edu.ucsc.dbtune.advisor.db2.DB2Advisor;
import edu.ucsc.dbtune.bip.core.IndexTuningOutput;
import edu.ucsc.dbtune.bip.indexadvisor.CoPhy;
import edu.ucsc.dbtune.bip.util.LogListener;
import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.optimizer.ExplainedSQLStatement;
import edu.ucsc.dbtune.optimizer.InumOptimizer;
import edu.ucsc.dbtune.optimizer.Optimizer;
import edu.ucsc.dbtune.util.Environment;
import edu.ucsc.dbtune.workload.Workload;

public class CoPhyFunctionalTest 
{
    private static DatabaseSystem db;
    private static Environment    en;
    
    private static double B;
    private static long totalIndexSize;
    private static Set<Index> candidates;
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
    public void testCoPhy() throws Exception
    {   
        workload = workload(en.getWorkloadsFoldername() + "/tpch");
        CandidateGenerator candGen = 
            new OptimizerCandidateGenerator(getBaseOptimizer(db.getOptimizer()));
        candidates = candGen.generate(workload);
        
        totalIndexSize = 0;
        for (Index index : candidates)
            totalIndexSize += index.getBytes();
        
        System.out.println("L51, number of candidate: " + candidates.size() + " size: " 
                            + totalIndexSize);
        
        B = Math.pow(2, 28);
        
        CoPhy cophy = new CoPhy();
        Optimizer io = db.getOptimizer();

        if (!(io instanceof InumOptimizer))
            throw new Exception("Expecting InumOptimizer instance");
                
        LogListener logger = LogListener.getInstance();
        cophy.setCandidateIndexes(candidates);
        cophy.setWorkload(workload); 
        cophy.setOptimizer((InumOptimizer) io);
        cophy.setSpaceBudget(B);
        cophy.setLogListenter(logger);
        
        IndexTuningOutput output = cophy.solve();
        
        if (output != null) {
            
            totalIndexSize = 0;
            for (Index index : output.getRecommendation())
                totalIndexSize += index.getBytes();
            
            System.out.println(" Objective value: " + cophy.getObjValue()
                                + "\n Running time: " + logger.toString());
            System.out.println(" Recommended indexes: " + output.getRecommendation().size() + "\n" 
                               + " total size: " + totalIndexSize
                               + output.getRecommendation());
            
            double costCoPhy, costDB2;
            
            List<Double> costCoPhys = new ArrayList<Double>();
            List<Double> costDB2s = new ArrayList<Double>();
            
            // compare with DB2Advis
            long start = System.currentTimeMillis();
            DB2Advisor db2advis = new DB2Advisor(db);
            db2advis.process(workload);                    
            Set<Index> db2Indexes = db2advis.getRecommendation();
            totalIndexSize = 0;
            for (Index index : db2Indexes)
                totalIndexSize += index.getBytes();
            
            System.out.println("Time to run DB2 Advisor: " + (System.currentTimeMillis() - start)
                                + " total index size: " + totalIndexSize);
            
            ExplainedSQLStatement cophyExplain;
            ExplainedSQLStatement db2Explain;
            
            costCoPhy = 0;
            costDB2 = 0;
            for (int i = 0; i < workload.size(); i++) {
                
                cophyExplain =  io.getDelegate().explain(workload.get(i), 
                                                        output.getRecommendation());
                
                db2Explain =  io.getDelegate().explain(workload.get(i), 
                                                       db2Indexes);
                                                  
                costCoPhys.add(cophyExplain.getTotalCost());
                costDB2s.add(db2Explain.getTotalCost());
                
                costCoPhy += cophyExplain.getTotalCost();
                costDB2 += db2Explain.getTotalCost();
            }
            
            System.out.println(" cost DB2: " + costDB2 + " vs. cost CoPhy: " + costCoPhy
                    + " ratio CoPhy / DB2: " + ((double) costCoPhy / costDB2));
            
        }
        
    }
}
