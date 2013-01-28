package edu.ucsc.dbtune.bip;

import static edu.ucsc.dbtune.DatabaseSystem.newDatabaseSystem;
import static edu.ucsc.dbtune.util.OptimizerUtils.getBaseOptimizer;
import static edu.ucsc.dbtune.util.TestUtils.workload;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.junit.BeforeClass;
import org.junit.Test;

import edu.ucsc.dbtune.DatabaseSystem;
import edu.ucsc.dbtune.advisor.candidategeneration.CandidateGenerator;
import edu.ucsc.dbtune.advisor.candidategeneration.OptimizerCandidateGenerator;
import edu.ucsc.dbtune.bip.core.IndexTuningOutput;
import edu.ucsc.dbtune.bip.indexadvisor.CoPhy;
import edu.ucsc.dbtune.bip.util.LogListener;
import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.optimizer.ExplainedSQLStatement;
import edu.ucsc.dbtune.optimizer.InumOptimizer;
import edu.ucsc.dbtune.optimizer.InumPreparedSQLStatement;
import edu.ucsc.dbtune.optimizer.Optimizer;
import edu.ucsc.dbtune.util.Environment;
import edu.ucsc.dbtune.workload.SQLStatement;

import static edu.ucsc.dbtune.workload.SQLCategory.NOT_SELECT;

public class CoPhyFunctionalTest 
{
    private static DatabaseSystem db;
    private static Environment    en;
    
    private static double B;
    private static long totalIndexSize;
    private static Set<Index> candidates;
    private static List<SQLStatement> workload;
    
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
        Optimizer io = db.getOptimizer();

        if (!(io instanceof InumOptimizer))
            return;

        workload = workload(en.getWorkloadsFoldername() + "/tpch-inum");
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
                               + " total size: " + totalIndexSize);
                               
            
            double costCoPhy = 0.0;            
            List<Double> costCoPhys = new ArrayList<Double>();
            ExplainedSQLStatement cophyExplain;
            InumPreparedSQLStatement inumStmt;
            for (int i = 0; i < workload.size(); i++) {
                
                inumStmt  = (InumPreparedSQLStatement) io.prepareExplain(workload.get(i));
                cophyExplain =  inumStmt.explain(output.getRecommendation());
                                                  
                costCoPhys.add(cophyExplain.getTotalCost());       
                
                costCoPhy += cophyExplain.getTotalCost();
                if (workload.get(i).getSQLCategory().isSame(NOT_SELECT)) 
                    costCoPhy -= cophyExplain.getBaseTableUpdateCost();
                 
            }
            
            System.out.println(" cost in INUM: " + costCoPhy
                                + " vs. cost by CPLEX: " + cophy.getObjValue()
                                + " RATIO: " + ((double) costCoPhy / cophy.getObjValue()));
             
        }
        
    }
}
