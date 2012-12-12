package edu.ucsc.dbtune;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.StringReader;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import edu.ucsc.dbtune.DatabaseSystem;
import edu.ucsc.dbtune.advisor.candidategeneration.CandidateGenerator;
import edu.ucsc.dbtune.advisor.candidategeneration.PowerSetOptimalCandidateGenerator;

import edu.ucsc.dbtune.advisor.candidategeneration.OptimizerCandidateGenerator;
import edu.ucsc.dbtune.bip.core.IndexTuningOutput;
import edu.ucsc.dbtune.bip.core.InumQueryPlanDesc;
import edu.ucsc.dbtune.bip.core.QueryPlanDesc;
import edu.ucsc.dbtune.bip.sim.RandomScheduler;
import edu.ucsc.dbtune.bip.sim.Schedule;
import edu.ucsc.dbtune.bip.sim.ScheduleOnOptimizer;
import edu.ucsc.dbtune.bip.sim.SimModel;
import edu.ucsc.dbtune.bip.util.LogListener;
import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.optimizer.DB2Optimizer;
import edu.ucsc.dbtune.optimizer.InumOptimizer;
import edu.ucsc.dbtune.optimizer.Optimizer;
import edu.ucsc.dbtune.seq.SeqCost;
import edu.ucsc.dbtune.seq.bip.SebBIPOutput;
import edu.ucsc.dbtune.seq.bip.SeqBIP;
import edu.ucsc.dbtune.seq.bip.SeqInumCost;
import edu.ucsc.dbtune.seq.bip.def.SeqInumIndex;
import edu.ucsc.dbtune.seq.bip.def.SeqInumPlan;
import edu.ucsc.dbtune.seq.bip.def.SeqInumQuery;
import edu.ucsc.dbtune.seq.bip.def.SeqInumSlot;
import edu.ucsc.dbtune.seq.bip.def.SeqInumSlotIndexCost;
import edu.ucsc.dbtune.seq.utils.RTimer;
import edu.ucsc.dbtune.seq.utils.RTimerN;
import edu.ucsc.dbtune.util.Rt;
import edu.ucsc.dbtune.util.Rx;
import edu.ucsc.dbtune.util.Environment;
import edu.ucsc.dbtune.workload.Workload;

import org.junit.BeforeClass;
import org.junit.Test;

import static edu.ucsc.dbtune.DatabaseSystem.newDatabaseSystem;
import static edu.ucsc.dbtune.bip.sim.SimVariablePool.VAR_X;
import static edu.ucsc.dbtune.bip.sim.SimVariablePool.VAR_Y;
import static edu.ucsc.dbtune.util.TestUtils.workload;
import static edu.ucsc.dbtune.util.TestUtils.getBaseOptimizer;

public class BipTest2 {
    public static DatabaseSystem db;
    public static Environment en;
    static int times = 1;
    static int indexSize = 10;

    public static Workload getWorkload(Environment en) throws IOException,
            SQLException {
        String file = "/tpch/workload_bip_seq.sql";
        file = "/tpch-small/workload.sql";
        file = "/tpcds-small/workload.sql";
        file = "/tpch/workload.sql";
        Workload workload = new Workload("", new FileReader(en
                .getWorkloadsFoldername()
                + file));
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < workload.size(); i++) {
            if (i != 0
            // && i != workload.size() - 1
            ) {
                for (int j = 0; j < times; j++)
                    sb.append(workload.get(i).getSQL() + ";\r\n");
                // break;
            }
        }
        return new Workload("", new StringReader(sb.toString()));
    }

    public static Set<Index> getIndexes(Workload workload, DatabaseSystem db)
            throws Exception {
        // Rt.runAndShowCommand("db2 connect to test");
        // HashSet<Index> set=new HashSet<Index>();
        // for (int i = 0; i <= 9; i++) {
        // Rt.runAndShowCommand("db2 set current query optimization = "+i);
        CandidateGenerator candGen = new OptimizerCandidateGenerator(
                getBaseOptimizer(db.getOptimizer()));
        Set<Index> indexes = candGen.generate(workload);
        // set.addAll(indexes);
        // Rt.p(indexes.size());
        // }
        // System.exit(0);

        // CandidateGenerator candGen = new PowerSetOptimalCandidateGenerator(
        // new OptimizerCandidateGenerator(getBaseOptimizer(db
        // .getOptimizer())), 3);
        // Set<Index> indexes = candGen.generate(workload);
        int size = indexSize;
        if (indexes.size() >= size) {
            Set<Index> temp = new HashSet<Index>();
            int count = 0;
            for (Index index : indexes) {
                temp.add(index);
                count++;
                if (count >= size)
                    break;
            }
            indexes = temp;
        }
        return indexes;
    }

    public static void testBIP() throws Exception {
        SeqCost.totalWhatIfNanoTime = 0;
        SeqCost.totalCreateIndexNanoTime = 0;
        SeqInumCost.populateTime = 0;
        SeqInumCost.plugInTime = 0;
        RTimerN timer = new RTimerN();

        en = Environment.getInstance();
        en.setProperty("username", "db2inst1");
        en.setProperty("password", "db2inst1admin");
        en.setProperty("workloads.dir", "resources/workloads/db2");
        String dbName = "test";
        en.setProperty("jdbc.url", "jdbc:db2://localhost:50000/" + dbName);
        db = newDatabaseSystem(en);

        Workload workload = BipTest2.getWorkload(en);
        Set<Index> indexes = BipTest2.getIndexes(workload, db);

        InumOptimizer optimizer = (InumOptimizer) db.getOptimizer();
        DB2Optimizer db2optimizer = (DB2Optimizer) optimizer.getDelegate();
        // timer.finish("loading");
        timer.reset();
        SeqInumCost cost = SeqInumCost.fromInum(db, optimizer, workload,
                indexes);
        if (false) {
            for (int i = 0; i < cost.queries.size(); i++) {
                Rt.p("Q" + i + ": ");
                for (SeqInumPlan plan : cost.queries.get(i).plans) {
                    Rt.np("\tPlan: " + plan.internalCost);
                    for (SeqInumSlot slot : plan.slots) {
                        Rt.np("\t\tSlot: " + slot.fullTableScanCost);
                        for (SeqInumSlotIndexCost c : slot.costs) {
                            Rt
                                    .np("\t\t\tIndex: " + c.index.name + " "
                                            + c.cost);
                        }
                    }
                }
            }
            for (int i = 0; i < cost.indices.size(); i++) {
                Rt.p("I" + i + ": " + cost.indices.get(i).createCost);
                cost.indices.get(i).storageCost = 1000;
            }
        }
        Rt.p("%d x %d", cost.queries.size(), cost.indices.size());
        double allTime = timer.getSecondElapse();
        double inumCreateIndexCostTime = SeqCost.totalCreateIndexNanoTime / 1000000000.0;
        double inumPopulateTime = SeqInumCost.populateTime;
        double inumPluginTime = SeqInumCost.plugInTime;
        Rt.p("INUM time: %.3f", timer.getSecondElapse());
        timer.reset();
        cost.storageConstraint = 2000;
        for (int i = 0; i < cost.indices.size(); i++) {
            cost.indices.get(i).storageCost = 1000;
        }
        SeqBIP bip = new SeqBIP(cost);
        bip.setOptimizer(optimizer);
        LogListener logger = LogListener.getInstance();
        bip.setLogListenter(logger);
        bip.setWorkload(new Workload("", new StringReader("")));
        SebBIPOutput output = (SebBIPOutput) bip.solve();
        double bipTime = timer.getSecondElapse();
        allTime += bipTime;
        double bipCost = bip.getObjValue();
        Rt.p("%d x %d", cost.queries.size(), cost.indices.size());
        Rt.p("cost: %,.0f", bip.getObjValue());
        Rt.p("BIP time: %.3f", timer.getSecondElapse());
        if (true)
            for (int i = 0; i < output.indexUsed.length; i++) {
                System.out.print("Query " + i + ":");
                for (SeqInumIndex index : output.indexUsed[i]) {
                    System.out.print(" " + index.name);
                }
                System.out.println();
            }
        if (true)
            for (SeqInumQuery query : cost.queries) {
                double c = 0;
                c += query.selectedPlan.internalCost;
                HashSet<Index> indexs = new HashSet<Index>();
                for (SeqInumSlot slot : query.selectedPlan.slots) {
                    if (slot.selectedIndex == null)
                        c += slot.fullTableScanCost;
                    else {
                        c += slot.selectedIndex.cost;
                        indexs.add(slot.selectedIndex.index.index);
                    }
                }
                double db2cost = db2optimizer.explain(query.sql, indexs)
                        .getTotalCost();
                Rt.p("%s q=%,.0f db2=%,.0f t=%,.0f", query.name, c, db2cost,
                        query.transitionCost);
                if (Math.abs(c - db2cost) > 1)
                    Rt.error("%.2f%%", c / db2cost * 100);
            }
        db.getConnection().close();

        Rx rx = new Rx("data");
        rx.createChild("queryCount", cost.queries.size());
        rx.createChild("indexCount", cost.indices.size());
        rx.createChild("createIndexCostTime", inumCreateIndexCostTime);
        rx.createChild("inumPluginTime", inumPluginTime);
        rx.createChild("inumPluginCount", SeqCost.plugInCount);
        rx.createChild("inumPopulateTime", inumPopulateTime);
        rx.createChild("allTime", allTime);
        rx.createChild("time", bipTime);
        rx.createChild("cost", bipCost);
        Rt.write(new File("/tmp/t.xml"), rx.getXml().toString());

    }

    static void testGREEDY() throws Exception {
        SeqWhatIfTest2.perf_createIndexCostTime = 0;
        SeqWhatIfTest2.perf_pluginTime = 0;
        SeqWhatIfTest2.perf_allTime = 0;
        SeqWhatIfTest2.perf_algorithmTime = 0;
        SeqCost.totalWhatIfNanoTime = 0;
        SeqCost.totalCreateIndexNanoTime = 0;
        SeqCost.plugInTime = 0;
        SeqCost.populateTime = 0;
        SeqWhatIfTest2 t = new SeqWhatIfTest2();
        Rx rx = new Rx("data");
        rx.createChild("queryCount", t.cost.sequence.length);
        rx.createChild("indexCount", t.cost.indicesV.size());
        rx.createChild("createIndexCostTime",
                SeqWhatIfTest2.perf_createIndexCostTime);
        rx.createChild("inumPluginTime", SeqWhatIfTest2.perf_pluginTime);
        rx.createChild("inumPopulateTime", SeqWhatIfTest2.perf_populateTime);
        rx.createChild("inumPluginCount", SeqCost.plugInCount);
        rx.createChild("allTime", SeqWhatIfTest2.perf_allTime);
        rx.createChild("time", SeqWhatIfTest2.perf_algorithmTime);
        rx.createChild("cost", SeqWhatIfTest2.perf_cost);
        Rt.write(new File("/tmp/t.xml"), rx.getXml().toString());
        // String log = String
        // .format(
        // "%d x %d,\t\t%.3f,%.3f,\t%.3f,%.3f,\t%.3f,%.3f,\t%.3f,%.3f,\t \"%,.0f\",\"%,.0f\"",
        // cost.sequence.length, cost.indicesV.size(),//
        // inumCreateIndexCostTime, //
        // SeqWhatIfTest2.perf_createIndexCostTime, //
        // inumPluginTime, //
        // SeqWhatIfTest2.perf_pluginTime,//
        // inumTime, //
        // SeqWhatIfTest2.perf_inumTime,//
        // bipTime,//
        // SeqWhatIfTest2.perf_algorithmTime, //
        // bipCost,//
        // SeqWhatIfTest2.perf_cost);
        // sb.append(log + "\r\n");
        // Rt.np("query x index,\t createCostTime,\t pluginTime,\t "
        // + "inumTime,\t time,\t cost");
        // Rt.np(sb);
    }

    public static StringBuilder sb = new StringBuilder();

    public static void main(String[] args) throws Exception {
        // WfitTest2.testBIP();
        args=new String[] {"1","100","bip"};
        times = Integer.parseInt(args[0]);
        indexSize = Integer.parseInt(args[1]);
        if ("bip".equals(args[2]))
            testBIP();
        else if ("greedy".equals(args[2]))
            testGREEDY();
        else
            throw new Error();
    }
}
