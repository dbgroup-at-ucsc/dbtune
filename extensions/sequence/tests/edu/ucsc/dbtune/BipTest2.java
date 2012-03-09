package edu.ucsc.dbtune;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.StringReader;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Set;

import edu.ucsc.dbtune.DatabaseSystem;
import edu.ucsc.dbtune.advisor.candidategeneration.CandidateGenerator;
import edu.ucsc.dbtune.advisor.candidategeneration.PowerSetOptimalCandidateGenerator;

import edu.ucsc.dbtune.advisor.candidategeneration.OptimizerCandidateGenerator;
import edu.ucsc.dbtune.bip.BIPTestConfiguration;
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
import edu.ucsc.dbtune.seq.utils.Rt;
import edu.ucsc.dbtune.util.Environment;
import edu.ucsc.dbtune.workload.SQLStatement;
import edu.ucsc.dbtune.workload.Workload;

import org.junit.BeforeClass;
import org.junit.Test;

import static edu.ucsc.dbtune.DatabaseSystem.newDatabaseSystem;
import static edu.ucsc.dbtune.bip.sim.SimVariablePool.VAR_X;
import static edu.ucsc.dbtune.bip.sim.SimVariablePool.VAR_Y;
import static edu.ucsc.dbtune.util.TestUtils.workload;
import static edu.ucsc.dbtune.util.TestUtils.getBaseOptimizer;

public class BipTest2 extends BIPTestConfiguration {
    private static DatabaseSystem db;
    private static Environment en;
    static int times = 3;

    public static Workload getWorkload(Environment en) throws IOException,
            SQLException {
        String file = "/tpch/workload_bip_seq.sql";
        file = "/tpch-small/workload.sql";
        Workload workload = new Workload("", new FileReader(en
                .getWorkloadsFoldername()
                + file));
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < workload.size(); i++) {
            if (i != 1 && i != workload.size() - 1) {
                for (int j = 0; j < times; j++)
                    sb.append(workload.get(i).getSQL() + ";\r\n");
            }
        }
        return new Workload("", new StringReader(sb.toString()));
    }

    public static Set<Index> getIndexes(Workload workload, DatabaseSystem db)
            throws SQLException {
        CandidateGenerator candGen = new OptimizerCandidateGenerator(
                getBaseOptimizer(db.getOptimizer()));
        Set<Index> indexes = candGen.generate(workload);

        // CandidateGenerator candGen = new PowerSetOptimalCandidateGenerator(
        // new OptimizerCandidateGenerator(getBaseOptimizer(db
        // .getOptimizer())), 3);
        // Set<Index> indexes = candGen.generate(workload);
        // for (Index index : indexes) {
        // if (index
        // .toString()
        // .equals(
        // "[+TPCH.LINEITEM.L_QUANTITY(A)+TPCH.LINEITEM.L_SHIPMODE(A)]")) {
        // Rt.p("remove " + index);
        // indexes.remove(index);
        // break;
        // }
        // }
        // int size = 50;
        // if (indexes.size() >= size) {
        // Set<Index> temp = new HashSet<Index>();
        // int count = 0;
        // for (Index index : indexes) {
        // temp.add(index);
        // count++;
        // if (count >= size)
        // break;
        // }
        // indexes = temp;
        // }
        return indexes;
    }

    public static void test() throws Exception {
        RTimerN timer = new RTimerN();

        en = Environment.getInstance();
        db = newDatabaseSystem(en);

        Workload workload = BipTest2.getWorkload(en);
        Set<Index> indexes = BipTest2.getIndexes(workload, db);

        InumOptimizer optimizer = (InumOptimizer) db.getOptimizer();
        DB2Optimizer db2optimizer = (DB2Optimizer) optimizer.getDelegate();
        // timer.finish("loading");
        timer.reset();
        SeqInumCost cost = SeqInumCost.fromInum(optimizer, workload, indexes);
        for (int i = 0; i < cost.sequence.length; i++) {
            Rt.p("Q" + i + ": ");
            for (SeqInumPlan plan : cost.sequence[i].plans) {
                Rt.np("\tPlan: " + plan.internalCost);
                for (SeqInumSlot slot : plan.slots) {
                    Rt.np("\t\tSlot: " + slot.fullTableScanCost);
                    for (SeqInumSlotIndexCost c : slot.costs) {
                        Rt.np("\t\t\tIndex: " + c.index.name + " " + c.cost);
                    }
                }
            }
        }
        for (int i = 0; i < cost.indicesV.size(); i++) {
            Rt.p("I" + i + ": " + cost.indicesV.get(i).createCost);
            cost.indicesV.get(i).storageCost = 1000;
        }
        Rt.p("%d x %d", cost.sequence.length, cost.indicesV.size());
        Rt.p("INUM time: %.3f", timer.getSecondElapse());
        timer.reset();
        cost.storageConstraint = 2000;
        SeqBIP bip = new SeqBIP(cost);
        bip.setOptimizer(optimizer);
        LogListener logger = LogListener.getInstance();
        bip.setLogListenter(logger);
        bip.setWorkload(new Workload("", new StringReader("")));
        SebBIPOutput output = (SebBIPOutput) bip.solve();
        for (int i = 0; i < output.indexUsed.length; i++) {
            System.out.print("Query " + i + ":");
            for (SeqInumIndex index : output.indexUsed[i]) {
                System.out.print(" " + index.name);
            }
            System.out.println();
        }
        Rt.p("%d x %d", cost.sequence.length, cost.indicesV.size());
        Rt.p("cost: %,.0f", bip.getObjValue());
        Rt.p("BIP time: %.3f", timer.getSecondElapse());
        for (SeqInumQuery query : cost.sequence) {
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

        SeqCost.totalWhatIfNanoTime = 0;
        db.getConnection().close();
        SeqWhatIfTest2.main(null);
    }

    public static void main(String[] args) throws Exception {
        times = 5;
        test();
    }
}
