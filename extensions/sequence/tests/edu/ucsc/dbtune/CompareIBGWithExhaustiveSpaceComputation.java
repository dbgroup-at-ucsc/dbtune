package edu.ucsc.dbtune;

import static com.google.common.collect.Sets.cartesianProduct;
import static edu.ucsc.dbtune.DatabaseSystem.newDatabaseSystem;
import static edu.ucsc.dbtune.inum.FullTableScanIndex.getFullTableScanIndexInstance;
import static edu.ucsc.dbtune.util.MetadataUtils.getIndexesPerTable;

import java.io.File;
import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import edu.ucsc.dbtune.inum.AbstractSpaceComputation;
import edu.ucsc.dbtune.inum.ExhaustiveSpaceComputation;
import edu.ucsc.dbtune.inum.IBGSpaceComputation;
import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.metadata.Table;
import edu.ucsc.dbtune.optimizer.DB2Optimizer;
import edu.ucsc.dbtune.optimizer.ExplainTables;
import edu.ucsc.dbtune.optimizer.InumOptimizer;
import edu.ucsc.dbtune.optimizer.plan.InumPlan;
import edu.ucsc.dbtune.optimizer.plan.InumPlanWithCache;
import edu.ucsc.dbtune.optimizer.plan.Operator;
import edu.ucsc.dbtune.seq.utils.RRange;
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
            space1.clear();
            space2.clear();
            RRange range = new RRange();
            RRange range2 = new RRange();
            RTimerN timer = new RTimerN();

            Set<? extends Index> indexes = AbstractSpaceComputation
                    .extractInterestingOrderFromDB(workload.get(i),
                            db2optimizer);
            List<Set<Index>> indexesPerTable = new ArrayList<Set<Index>>();

            for (Map.Entry<Table, Set<Index>> e : getIndexesPerTable(indexes)
                    .entrySet()) {
                e.getValue().add(getFullTableScanIndexInstance(e.getKey()));
                indexesPerTable.add(e.getValue());
            }

            int expectedWhatIfCalls = cartesianProduct(indexesPerTable).size();
            boolean runExhaustive = expectedWhatIfCalls < 100;
            int count = ExplainTables.whatIfCallCount;
            boolean timeout = false;
            boolean error = false;
            try {
                ibg.compute(space1, workload.get(i), db2optimizer, db
                        .getCatalog());
                for (InumPlan plan : space1) {
                    plan = new InumPlanWithCache(plan,
                            new HashMap<String, Operator>());
                    range.add(plan.getSlots().size());
                    range2.add(plan.getTables().size());
                }
            } catch (Exception e) {
                if ("IBG timeout".equals(e.getMessage()))
                    timeout = true;
                else
                    error = true;
            }
            double time1 = timer.getSecondElapse();
            int count1 = ExplainTables.whatIfCallCount - count;
            timer.reset();
            count = ExplainTables.whatIfCallCount;
            if (runExhaustive)
                exhaustive.compute(space2, workload.get(i), db2optimizer, db
                        .getCatalog());
            double time2 = timer.getSecondElapse();
            int count2 = ExplainTables.whatIfCallCount - count;
            System.out.print(i + "\t");
            if (timeout)
                System.out.print("timeout\t");
            else if (error)
                System.out.print("error\t");
            else
                System.out.format("%.3f\t", time1);
            if (runExhaustive)
                System.out.format("%.3f\t", time2);
            else
                System.out.format("\t");
            System.out.format("%d\t", count1);
            if (runExhaustive)
                System.out.format("%d\t", count2);
            else
                System.out.format("\t");
            System.out.format("%d\t", space1.size());
            if (runExhaustive)
                System.out.format("%d\t", space2.size());
            else
                System.out.format("\t");
            System.out.format("%d\t%d\t", indexes.size(), expectedWhatIfCalls);
            if (!timeout && !error)
                System.out.format("%.0f/%.0f\t%.0f/%.0f", range.min, range.max,
                        range2.min, range2.max);
            System.out.println();

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
