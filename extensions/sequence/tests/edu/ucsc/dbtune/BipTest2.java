package edu.ucsc.dbtune;

import java.sql.SQLException;
import java.util.HashSet;
import java.util.Set;

import edu.ucsc.dbtune.DatabaseSystem;
import edu.ucsc.dbtune.advisor.candidategeneration.CandidateGenerator;

import edu.ucsc.dbtune.advisor.candidategeneration.OptimizerCandidateGenerator;
import edu.ucsc.dbtune.bip.BIPTestConfiguration;
import edu.ucsc.dbtune.bip.core.IndexTuningOutput;
import edu.ucsc.dbtune.bip.sim.RandomScheduler;
import edu.ucsc.dbtune.bip.sim.Schedule;
import edu.ucsc.dbtune.bip.sim.ScheduleOnOptimizer;
import edu.ucsc.dbtune.bip.sim.SimModel;
import edu.ucsc.dbtune.bip.util.LogListener;
import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.optimizer.InumOptimizer;
import edu.ucsc.dbtune.optimizer.Optimizer;
import edu.ucsc.dbtune.seq.bip.SeqBIP;
import edu.ucsc.dbtune.util.Environment;
import edu.ucsc.dbtune.workload.SQLStatement;
import edu.ucsc.dbtune.workload.Workload;

import org.junit.BeforeClass;
import org.junit.Test;

import static edu.ucsc.dbtune.DatabaseSystem.newDatabaseSystem;
import static edu.ucsc.dbtune.util.TestUtils.workload;
import static edu.ucsc.dbtune.util.TestUtils.getBaseOptimizer;

/**
 * Test for the InteractionBIP class.
 * 
 * @author Quoc Trung Tran
 */
public class BipTest2 extends BIPTestConfiguration {
    private static DatabaseSystem db;
    private static Environment en;

    /**
     * The test has to check first just one query and one index.
     * 
     * @throws Exception
     *             if an I/o error occurs; if a DBMS communication failure
     *             occurs
     */
    @Test
    public void testScheduling() throws Exception {
//        en = Environment.getInstance();
//        db = newDatabaseSystem(en);
//
//        System.out.println(" In test scheduling ");
//        Workload workload = workload(en.getWorkloadsFoldername()
//                + "/tpch-small");
//
//        // 2. powerset
//        CandidateGenerator candGen = // new PowerSetOptimalCandidateGenerator(
//        new OptimizerCandidateGenerator(getBaseOptimizer(db.getOptimizer()));// ,
//                                                                             // 3);
//
//        // CandidateGenerator candGen = new
//        // PowerSetCandidateGenerator(db.getCatalog(), 2, true);
//        Set<Index> indexes = candGen.generate(workload);
//
//        if (indexes.size() >= 200) {
//            Set<Index> temp = new HashSet<Index>();
//            int count = 0;
//            for (Index index : indexes) {
//                temp.add(index);
//                count++;
//                if (count >= 200)
//                    break;
//            }
//            indexes = temp;
//        }
//
//        System.out.println("Number of indexes: " + indexes.size()
//                + " number of statements: " + workload.size());
//
//        for (Index index : indexes)
//            System.out.println("Index : " + index);
//
//        Set<Index> Sinit = new HashSet<Index>();
//        Set<Index> Smat = new HashSet<Index>();
//        Smat = indexes;
//
//        Optimizer io = db.getOptimizer();
//
//        if (!(io instanceof InumOptimizer))
//            throw new Exception("Expecting InumOptimizer instance");
//
//        int numIndexesWindow = 10;
//        LogListener logger = LogListener.getInstance();
//        SeqBIP bip = new SeqBIP();
//        bip.setOptimizer((InumOptimizer) io);
//        bip.setWorkload(workload);
//        bip.setLogListenter(logger);
//        bip.setConfigurations(Sinit, Smat);
//        bip.setNumberofIndexesEachWindow(numIndexesWindow);
//        bip.setNumberWindows(indexes.size() / numIndexesWindow + 1);
//
//        Set<SQLStatement> sqls = new HashSet<SQLStatement>();
//        for (int i = 0; i < workload.size(); i++)
//            sqls.add(workload.get(i));
//
//        Schedule schedule = new Schedule(0, new HashSet<Index>());
//        schedule = (Schedule) bip.solve();
//        double bipCost, randomCost;
//
//        if (schedule != null) {
//
//            System.out.println("Result: " + schedule.toString());
//            System.out.println(logger.toString());
//
//            // invoke the actual optimizer
//            ScheduleOnOptimizer mso = new ScheduleOnOptimizer();
//            mso.verify(io.getDelegate(), schedule, sqls);
//            bipCost = mso.getTotalCost();
//            System.out.println(" Cost by BIP: " + bip.getObjValue()
//                    + " vs. cost by DB2: " + bipCost + " The RATIO: "
//                    + bip.getObjValue() / bipCost);
//
//            // compute the random schedule
//            RandomScheduler randomSchedule = new RandomScheduler();
//            randomSchedule.setConfigurations(Sinit, Smat);
//            randomSchedule.setNumberofIndexesEachWindow(1);
//            randomSchedule.setNumberWindows(indexes.size());
//            mso.verify(io.getDelegate(), (Schedule) randomSchedule.solve(),
//                    sqls);
//            randomCost = mso.getTotalCost();
//
//            System.out.println("L112, BIP cost: " + bipCost
//                    + " vs. RANDOM cost: " + randomCost + " RATIO: " + bipCost
//                    / randomCost);
//
//        }
    }
}
