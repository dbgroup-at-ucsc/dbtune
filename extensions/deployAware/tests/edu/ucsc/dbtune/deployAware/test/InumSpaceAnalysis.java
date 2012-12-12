package edu.ucsc.dbtune.deployAware.test;

import static edu.ucsc.dbtune.DatabaseSystem.newDatabaseSystem;

import java.io.PrintStream;
import java.io.StringReader;
import java.util.HashSet;
import java.util.Vector;

import edu.ucsc.dbtune.DatabaseSystem;
import edu.ucsc.dbtune.InumTest2;
import edu.ucsc.dbtune.deployAware.test.DATPaper.TestSet;
import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.optimizer.DB2Optimizer;
import edu.ucsc.dbtune.optimizer.InumOptimizer;
import edu.ucsc.dbtune.optimizer.InumPreparedSQLStatement;
import edu.ucsc.dbtune.optimizer.plan.InumPlan;
import edu.ucsc.dbtune.seq.bip.SeqInumCost;
import edu.ucsc.dbtune.seq.bip.WorkloadLoader;
import edu.ucsc.dbtune.seq.bip.def.SeqInumPlan;
import edu.ucsc.dbtune.seq.bip.def.SeqInumQuery;
import edu.ucsc.dbtune.seq.bip.def.SeqInumSlot;
import edu.ucsc.dbtune.seq.bip.def.SeqInumSlotIndexCost;
import edu.ucsc.dbtune.util.Environment;
import edu.ucsc.dbtune.util.Rt;
import edu.ucsc.dbtune.workload.Workload;

public class InumSpaceAnalysis {
    static void show(SeqInumPlan plan) {
        plan.sortSlots();
        Rt.np(plan.id + " " + plan.internalCost);
        for (SeqInumSlot slot : plan.slots) {
            Rt.np("\t" + slot.fullTableScanCost);
            for (SeqInumSlotIndexCost c : slot.costs) {
                Rt.np("\t\t"+c.cost+"\t" + c.index.indexStr);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        long gigbytes = 1024L * 1024L * 1024L;
        TestSet[] sets = { new TestSet("40 TPCDS queries", "test",
                "deployAware", "tpcds_40.sql", 10 * gigbytes, "TPCDS40",0),
        // new TestSet("16 TPC-H queries", "tpch10g", "deployAware",
        // "TPCH16.sql", 10 * gigbytes, "TPCH"),
        // new TestSet("63 TPCDS queries", "test", "deployAware",
        // "TPCDS63.sql", 10 * gigbytes, "TPCDS63"),
        // new TestSet("81 OTAB queries", "test", "deployAware",
        // "OTAB86.sql", 10 * gigbytes, "OTAB86"),
        };
        String generateIndexMethod = "recommend";

        int total = 0;
        int n = 0;
        int removedIndexes = 0;
        int totalIndexes = 0;
        SeqInumPlan.maxAllowedError = 0.1;
        for (TestSet set : sets) {
            Rt.p(set.dbName + " " + set.workloadName + " " + set.fileName);
            PrintStream ps = System.out;
            WorkloadLoader loader = new WorkloadLoader(set.dbName,
                    set.workloadName, set.fileName, generateIndexMethod);
            SeqInumCost cost = loader.loadCost();
            for (SeqInumQuery query : cost.queries) {
                total += query.plans.length;
                Rt.np("Query: " + query.id + " " + query.plans.length);
                // SeqInumQuery query = cost.queries.get(2);
                // Environment en = Environment.getInstance();
                // en.setProperty("username", "db2inst1");
                // en.setProperty("password", "db2inst1admin");
                // en.setProperty("workloads.dir", "resources/workloads/db2");
                // en.setProperty("jdbc.url", "jdbc:db2://localhost:50000/"
                // + "tpch10g");
                // DatabaseSystem tpch10g = newDatabaseSystem(en);
                // InumOptimizer optimizer = (InumOptimizer)
                // tpch10g.getOptimizer();
                // DB2Optimizer db2optimizer = (DB2Optimizer)
                // optimizer.getDelegate();
                // HashSet<Index> indexes = new HashSet<Index>();
                // indexes.add(InumTest2.createIndex(tpch10g,
                // "+TPCH.NATION.N_REGIONKEY(A)+TPCH.NATION.N_NATIONKEY(A)"));
                // indexes
                // .add(InumTest2
                // .createIndex(tpch10g,
                // "+TPCH.NATION.N_REGIONKEY(A)+TPCH.NATION.N_NAME(A)+TPCH.NATION.N_NATIONKEY(A)"));
                // Workload workload = new Workload("", new StringReader(loader
                // .getWorkload().get(5).getSQL()
                // + ";"));
                // {
                // // InumPreparedSQLStatement space;
                // // space = (InumPreparedSQLStatement) optimizer
                // // .prepareExplain(workload.get(0));
                // // Rt.p("Plans: " + space.getTemplatePlans().size());
                // // for (InumPlan plan : space.getTemplatePlans()) {
                // // Rt.p(plan);
                // // }
                // }
                // SeqInumCost cost = SeqInumCost.fromInum(tpch10g, optimizer,
                // workload, indexes);
                 query = cost.queries.get(2);
                for (SeqInumPlan plan : query.plans) {
                    totalIndexes += plan.totalIndexes();
                    plan.removeDupIndexes();
                    removedIndexes += plan.totalIndexes();
                     show(plan);
                }
                next: for (SeqInumPlan plan1 : query.plans) {
                    for (SeqInumPlan plan2 : query.plans) {
                        if (plan1.id >= plan2.id)
                            continue;
                        if (plan1.isCoveredBy(plan2)) {
                            Rt.p(query.id + " " + plan1.id + " " + plan2.id);
                            n++;
                            continue next;
                            // show(plan1);
                            // show(plan2);
                        }
                    }
                }
                 break;
            }
        }
        Rt.np(n + "/" + total);
        removedIndexes=totalIndexes-removedIndexes;
        Rt.np(removedIndexes + "/" + totalIndexes);
    }
}
