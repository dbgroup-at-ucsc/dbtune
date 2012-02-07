package edu.ucsc.dbtune.bip;

import java.io.FileReader;
import java.util.HashSet;
import java.util.Set;

import edu.ucsc.dbtune.DatabaseSystem;
import edu.ucsc.dbtune.bip.core.CPlexSolver;
import edu.ucsc.dbtune.bip.core.CandidateGenerator;
import edu.ucsc.dbtune.bip.core.DB2CandidateGenerator;
import edu.ucsc.dbtune.bip.sim.MaterializationSchedule;
import edu.ucsc.dbtune.bip.sim.MaterializationScheduleOnOptimizer;
import edu.ucsc.dbtune.bip.sim.SimBIP;
import edu.ucsc.dbtune.bip.util.LogListener;
import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.optimizer.InumOptimizer;
import edu.ucsc.dbtune.optimizer.Optimizer;
import edu.ucsc.dbtune.util.Environment;
import edu.ucsc.dbtune.workload.SQLStatement;
import edu.ucsc.dbtune.workload.Workload;

import org.junit.Test;

import static edu.ucsc.dbtune.DatabaseSystem.newDatabaseSystem;

/**
 * Test SimBIP.
 *
 * @author tqtrung
 */
public class SimBIPTest 
{   
    private static DatabaseSystem db;
    private static Environment    en;

    /**
     * The test has to check first just one query and one index.     
     * 
     * @throws Exception
     *      if an I/o error occurs; if a DBMS communication failure occurs
     */
    @Test
    public void testScheduling() throws Exception
    {
        en  = Environment.getInstance();
        db = newDatabaseSystem(en);
        
        System.out.println(" In test scheduling ");
        String workloadFile   = en.getScriptAtWorkloadsFolder("tpch/smallworkload.sql");
        FileReader fileReader = new FileReader(workloadFile);
        Workload workload     = new Workload(fileReader);
        Set<Index> indexes;
        
        CandidateGenerator generator = new DB2CandidateGenerator();
        generator.setOptimizer(db.getOptimizer());
        generator.setWorkload(workload);
        indexes = generator.optimalCandidateSet();
        
        System.out.println("Number of indexes: " + indexes.size() 
                            + " number of statements: " + workload.size());
        for (Index index : indexes) 
            System.out.println("Index : " + index.columns()); 
        
        
        Set<Index> Sinit = new HashSet<Index>();
        Set<Index> Smat = new HashSet<Index>();
        Smat = indexes;
        
        Optimizer io = db.getOptimizer();

        if (!(io instanceof InumOptimizer))
            throw new Exception("Expecting InumOptimizer instance");
        
        LogListener logger = LogListener.getInstance();
        SimBIP bip = new SimBIP();
        bip.setOptimizer((InumOptimizer) io);
        bip.setWorkload(workload);
        bip.setLogListenter(logger);
        bip.setConfigurations(Sinit, Smat);
        bip.setNumberofIndexesEachWindow(1);
        bip.setNumberWindows(indexes.size());
        
        Set<SQLStatement> sqls = new HashSet<SQLStatement>();
        for (int i = 0; i < workload.size(); i++)
            sqls.add(workload.get(i));
        
        MaterializationSchedule schedule = new MaterializationSchedule (0, new HashSet<Index>());
        schedule = (MaterializationSchedule) bip.solve();
        if (schedule != null) {
            CPlexSolver cplex = bip.getSolver();
            // invoke the actual optimizer
            MaterializationScheduleOnOptimizer mso = new MaterializationScheduleOnOptimizer();
            mso.verify(io, schedule, sqls);
            System.out.println(" Cost by BIP: " + cplex.getObjectiveValue()
                               + " vs. cost by DB2: " + mso.getTotalCost()
                               + " The RATIO: " + cplex.getObjectiveValue() / mso.getTotalCost());
            
            System.out.println("Solver information: " + cplex);            
            System.out.println("Result: " + schedule.toString());
            System.out.println(logger.toString());
        }
    }
}
