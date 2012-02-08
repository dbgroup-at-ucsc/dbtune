package edu.ucsc.dbtune.bip;

import static edu.ucsc.dbtune.DatabaseSystem.newDatabaseSystem;
import java.io.FileReader;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Set;

import org.junit.Test;

import edu.ucsc.dbtune.DatabaseSystem;
import edu.ucsc.dbtune.bip.core.BIPOutput;
import edu.ucsc.dbtune.bip.core.CandidateGenerator;
import edu.ucsc.dbtune.bip.core.DB2CandidateGenerator;
import edu.ucsc.dbtune.bip.interactions.InteractionBIP;
import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.optimizer.InumOptimizer;
import edu.ucsc.dbtune.optimizer.Optimizer;
import edu.ucsc.dbtune.util.Environment;
import edu.ucsc.dbtune.workload.SQLStatement;
import edu.ucsc.dbtune.workload.Workload;

import edu.ucsc.dbtune.bip.util.LogListener;

/**
 * Test for the InteractionBIP class
 */
public class InteractionBIPFunctionalTest extends BIPTestConfiguration
{  
    private static DatabaseSystem db;
    private static Environment    en;
    
    @Test
    public void testInteraction() throws Exception
    {
        en  = Environment.getInstance();
        db = newDatabaseSystem(en);
        
        System.out.println(" In test interaction ");
        String workloadFile   = en.getScriptAtWorkloadsFolder("tpch/smallworkload.sql");
        FileReader fileReader = new FileReader(workloadFile);
        Workload workload     = new Workload(fileReader);
        
        CandidateGenerator generator = new DB2CandidateGenerator();
        generator.setWorkload(workload);
        generator.setOptimizer(db.getOptimizer());
        Set<Index> candidates = generator.oneColumnCandidateSet();
                
        System.out.println("L60 (Test), Number of indexes: " + candidates.size());
        for (Index index : candidates) 
            System.out.println("L62, Index: " + index.getId() + " " + index); 
        

        try {
            double delta = -0.1;
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
            
            BIPOutput output = bip.solve();
            System.out.println(output.toString());
            System.out.println(logger.toString());
        } catch (SQLException e) {
            System.out.println(" error " + e.getMessage());
        }
    }
}
