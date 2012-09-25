package edu.ucsc.dbtune;

import static edu.ucsc.dbtune.DatabaseSystem.newDatabaseSystem;

import java.io.File;
import java.io.IOException;
import java.io.StringReader;
import java.util.HashSet;
import java.util.Set;

import edu.ucsc.dbtune.inum.ExhaustiveSpaceComputation;
import edu.ucsc.dbtune.inum.IBGSpaceComputation;
import edu.ucsc.dbtune.optimizer.DB2Optimizer;
import edu.ucsc.dbtune.optimizer.ExplainTables;
import edu.ucsc.dbtune.optimizer.InumOptimizer;
import edu.ucsc.dbtune.optimizer.plan.InumPlan;
import edu.ucsc.dbtune.seq.utils.RTimer;
import edu.ucsc.dbtune.seq.utils.RTimerN;
import edu.ucsc.dbtune.util.Environment;
import edu.ucsc.dbtune.util.Rt;
import edu.ucsc.dbtune.workload.Workload;

public class CompareIBGWithExhaustiveSpaceComputation {
    static void compareWorkload(DatabaseSystem db, String query, String name)
            throws Exception {
        InumOptimizer optimizer = (InumOptimizer) db.getOptimizer();
        DB2Optimizer db2optimizer = (DB2Optimizer) optimizer.getDelegate();

        IBGSpaceComputation ibg = new IBGSpaceComputation();
        ExhaustiveSpaceComputation exhaustive = new ExhaustiveSpaceComputation();
        Set<InumPlan> space1 = new HashSet<InumPlan>();
        Set<InumPlan> space2 = new HashSet<InumPlan>();

        Workload workload = new Workload("", new StringReader(query));

        Rt.np(name);

        for (int i = 0; i < workload.size(); i++) {
            try {
                RTimerN timer = new RTimerN();
                int count = ExplainTables.whatIfCallCount;
                ibg.compute(space1, workload.get(i), db2optimizer, db
                        .getCatalog());
                double time1 = timer.getSecondElapse();
                int count1 = ExplainTables.whatIfCallCount - count;
                timer.reset();
                count = ExplainTables.whatIfCallCount;
                exhaustive.compute(space2, workload.get(i), db2optimizer, db
                        .getCatalog());
                double time2 = timer.getSecondElapse();
                int count2 = ExplainTables.whatIfCallCount - count;
                Rt.np("%d\t%.3f\t%.3f\t%d\t%d\t%d\t%d", i, time1, time2,
                        count1, count2, space1.size(), space2.size());
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Environment en = Environment.getInstance();
        String dbName = "test";
        en.setProperty("jdbc.url", "jdbc:db2://localhost:50000/" + dbName);
        en.setProperty("username", "db2inst1");
        en.setProperty("password", "db2inst1admin");
        en.setProperty("workloads.dir", "resources/workloads/db2");
        DatabaseSystem test = newDatabaseSystem(en);
        dbName = "tpch10g";
        en.setProperty("jdbc.url", "jdbc:db2://localhost:50000/" + dbName);
        DatabaseSystem tpch10g = newDatabaseSystem(en);

        IBGSpaceComputation.maxTime = 15000;
        String tpch = Rt.readFile(new File(
                "resources/workloads/db2/tpch/complete.sql"));
        String tpcds = Rt.readFile(new File(
                "resources/workloads/db2/tpcds/db2.sql"));

        compareWorkload(tpch10g, tpch, "tpch");
        compareWorkload(test, tpcds, "tpcds");

        test.getConnection().close();
        tpch10g.getConnection().close();
    }
}
