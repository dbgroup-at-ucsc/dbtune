package edu.ucsc.dbtune.bip;

import java.sql.SQLException;
import java.util.Set;

import edu.ucsc.dbtune.DatabaseSystem;
import edu.ucsc.dbtune.advisor.candidategeneration.CandidateGenerator;
import edu.ucsc.dbtune.advisor.candidategeneration.OneColumnCandidateGenerator;
import edu.ucsc.dbtune.advisor.candidategeneration.OptimizerCandidateGenerator;
import edu.ucsc.dbtune.advisor.candidategeneration.PowerSetCandidateGenerator;
import edu.ucsc.dbtune.bip.core.IndexTuningOutput;
import edu.ucsc.dbtune.bip.interactions.InteractionBIP;
import edu.ucsc.dbtune.bip.util.LogListener;
import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.optimizer.InumOptimizer;
import edu.ucsc.dbtune.optimizer.Optimizer;
import edu.ucsc.dbtune.util.Environment;
import edu.ucsc.dbtune.workload.Workload;

import org.junit.Test;

import static edu.ucsc.dbtune.DatabaseSystem.newDatabaseSystem;
import static edu.ucsc.dbtune.util.TestUtils.getBaseOptimizer;
import static edu.ucsc.dbtune.util.TestUtils.workload;

/**
 * Test for the InteractionBIP class.
 *
 * @author Quoc Trung Tran
 */
public class InteractionBIPFunctionalTest extends BIPTestConfiguration
{  
    private static DatabaseSystem db;
    private static Environment    en;
    
    /**
     * @throws Exception
     *      if fails
     */
    @Test
    public void testInteraction() throws Exception
    {
        en = Environment.getInstance();
        db = newDatabaseSystem(en);
        
        System.out.println(" In test interaction ");
        Workload workload = workload(en.getWorkloadsFoldername() + "/tpch-small");
        
        CandidateGenerator candGen =
         //   new OneColumnCandidateGenerator(
                    new OptimizerCandidateGenerator(getBaseOptimizer(db.getOptimizer()));
        
        //CandidateGenerator candGen = new PowerSetCandidateGenerator(db.getCatalog(), 2, true);
        Set<Index> candidates = candGen.generate(workload);
        
        System.out.println("L60 (Test), Number of indexes: " + candidates.size() 
                            + " Number of statements: " + workload.size());
        for (Index index : candidates) 
            System.out.println("L62, Index: " + index.getId() + " " + index); 
        
        try {
            double delta = 0.1;
            Optimizer io = db.getOptimizer();

            if (!(io instanceof InumOptimizer))
                throw new Exception("Expecting InumOptimizer instance");
            
            LogListener logger = LogListener.getInstance();
            InteractionBIP bip = new InteractionBIP(delta);            
            bip.setCandidateIndexes(candidates);
            bip.setWorkload(workload);
            bip.setOptimizer((InumOptimizer) io);
            bip.setLogListenter(logger);
            bip.setConventionalOptimizer(io.getDelegate());
            
            IndexTuningOutput output = bip.solve();
            System.out.println(output.toString());
            System.out.println(logger.toString());
            
        } catch (SQLException e) {
            System.out.println(" error " + e.getMessage());
            throw e;
        }
    }
}
