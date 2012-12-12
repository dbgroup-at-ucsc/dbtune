package edu.ucsc.dbtune;

import static edu.ucsc.dbtune.util.TestUtils.getBaseOptimizer;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.StringReader;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import edu.ucsc.dbtune.DatabaseSystem;
import edu.ucsc.dbtune.advisor.candidategeneration.CandidateGenerator;
import edu.ucsc.dbtune.advisor.candidategeneration.OptimizerCandidateGenerator;
import edu.ucsc.dbtune.inum.DerbyInterestingOrdersExtractor;
import edu.ucsc.dbtune.metadata.Catalog;
import edu.ucsc.dbtune.metadata.Column;
import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.metadata.Schema;
import edu.ucsc.dbtune.metadata.Table;
import edu.ucsc.dbtune.optimizer.Optimizer;
import edu.ucsc.dbtune.seq.SeqCost;
import edu.ucsc.dbtune.seq.SeqGreedySeq;
import edu.ucsc.dbtune.seq.SeqMerge;
import edu.ucsc.dbtune.seq.SeqOptimal;
import edu.ucsc.dbtune.seq.SeqSplit;
import edu.ucsc.dbtune.seq.bip.SeqInumCost;
import edu.ucsc.dbtune.seq.bip.WorkloadLoader;
import edu.ucsc.dbtune.seq.def.*;
import edu.ucsc.dbtune.seq.utils.RTimer;
import edu.ucsc.dbtune.seq.utils.RTimerN;
import edu.ucsc.dbtune.util.Rt;
import edu.ucsc.dbtune.util.Environment;
import edu.ucsc.dbtune.workload.SQLStatement;
import edu.ucsc.dbtune.workload.Workload;

public class SeqWhatIfTest2 {
    SeqCost cost;
    SeqConfiguration[] allConfigurations;

    public SeqWhatIfTest2() throws Exception {
        // from file test
        if (false) {
            cost = SeqCost.fromFile(Rt.readResourceAsString(SeqCost.class,
                    "disjoint.txt"));
            SeqSplit split = new SeqSplit(cost, cost.sequence);
            for (SeqSplitGroup g : split.groups) {
                SeqConfiguration[] confs = cost.getAllConfigurations(g.indices);
                SeqStep[] steps = SeqOptimal.getOptimalSteps(cost.source,
                        cost.destination, g.queries, confs);
                SeqOptimal optimal = new SeqOptimal(cost, cost.source,
                        cost.destination, g.queries, steps);
                g.bestPath = optimal.getBestSteps();
                Rt.np(SeqOptimal.formatBestPathPlain(g.bestPath));
            }
            SeqMerge merge = new SeqMerge(cost, split.groups);
        }
        RTimerN timer = new RTimerN();
//        Environment en = Environment.getInstance();
//        en.setProperty("optimizer", "dbms");
//        en.setProperty("optimizer", "inum");
//        DatabaseSystem db = DatabaseSystem.newDatabaseSystem(en);
//        Optimizer optimizer = db.getOptimizer();

        // Workload workload = BipTest2.getWorkload(en);
        // Set<Index> indexes = BipTest2.getIndexes(workload, db);

        // List<Set<Index>> indexesPerTable;
        // DerbyInterestingOrdersExtractor interestingOrdersExtractor;
        //
        // interestingOrdersExtractor = new DerbyInterestingOrdersExtractor(db
        // .getCatalog(), true);
        // indexesPerTable = interestingOrdersExtractor
        // .extract(new SQLStatement(
        // "select\n"
        // + "            n_name as nation,\n"
        // + "            year(o_orderdate) as o_year,\n"
        // +
        // "            l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity as amount\n"
        // + "        from\n" + "            tpch.part,\n"
        // + "            tpch.supplier,\n"
        // + "            tpch.lineitem,\n"
        // + "            tpch.partsupp,\n"
        // + "            tpch.orders,\n"
        // + "            tpch.nation\n"
        // + "        where\n"
        // + "            s_suppkey = l_suppkey\n"
        // + "            and ps_suppkey = l_suppkey\n"
        // + "            and ps_partkey = l_partkey\n"
        // + "            and p_partkey = l_partkey\n"
        // + "            and o_orderkey = l_orderkey\n"
        // + "            and s_nationkey = n_nationkey\n"
        // + "            and p_name like '%thistle%'"));
        // HashSet<Index> allIndex = new HashSet<Index>();
        // for (Set<Index> set : indexesPerTable)
        // for (Index index : set)
        // allIndex.add(index);

        // timer.finish("loading");
        timer.reset();

        // Index[] indices = indexes.toArray(new Index[indexes.size()]);
        // cost = SeqCost.fromOptimizer(db, optimizer, workload, indices);
        WorkloadLoader loader = new WorkloadLoader("tpch10g", "deployAware",
                "TPCH16.sql", "recommend");
        cost = SeqCost.fromInum(loader.loadCost());
        cost.storageConstraint = Double.MAX_VALUE;//2000;
//        for (SeqIndex index : cost.indicesV)
//            index.createCost=10;
        if (false) {
            int n = 0;
            for (SeqQuery query : cost.sequence) {
                System.out.println((n++) + " " + query);
            }
            n = 0;
            for (SeqIndex index : cost.indicesV) {
                index.storageCost = 1000;
                System.out.println((n++) + " " + index);
            }
        }
        // timer.finish("calculating create index cost");
        // timer.reset();

        SeqGreedySeq greedySeq = new SeqGreedySeq(cost, cost.sequence,
                cost.indicesV.toArray(new SeqIndex[cost.indicesV.size()]));
        while (greedySeq.run())
            ;
        greedySeq.finish();
        if (true)
            Rt.np(SeqOptimal.formatBestPathPlain(greedySeq.bestPath));
        Rt.p("%d x %d", cost.sequence.length, cost.indicesV.size());
        Rt
                .p(
                        "GREEDY cost: %,.0f",
                        greedySeq.bestPath[greedySeq.bestPath.length - 1].costUtilThisStep);
        perf_cost = greedySeq.bestPath[greedySeq.bestPath.length - 1].costUtilThisStep;
        perf_allTime = timer.getSecondElapse();
        perf_algorithmTime = perf_allTime
                - ((SeqCost.totalWhatIfNanoTime + SeqCost.totalCreateIndexNanoTime) / 1000000000.0);
        perf_createIndexCostTime = SeqCost.totalCreateIndexNanoTime / 1000000000.0;
        perf_pluginTime = SeqCost.plugInTime / 1000000000.0;
        perf_populateTime = SeqCost.populateTime;
        Rt.p("GREEDY SEQ time %.3f", timer.getSecondElapse());
        Rt.p("WhatIf Call time %.3f",
                SeqCost.totalWhatIfNanoTime / 1000000000.0);
        // SeqStep[] steps = SeqOptimal.getOptimalSteps(cost.source,
        // cost.destination, cost.sequence, cost
        // .getAllConfigurations(cost.indicesV));
        // SeqOptimal optimal = new SeqOptimal(cost, cost.source,
        // cost.destination, cost.sequence, steps);
        // for (SeqStepConf step : optimal.getBestSteps()) {
        // if (step.step.query != null)
        // Rt.np(step.step.query.name + ": " + step.step.query.sql.trim());
        // Rt.np("\t" + step.configuration);
        // Rt.np("\t" + step.costUtilThisStep);
        // }

//        db.getConnection().close();
    }

    public static double perf_cost;
    public static double perf_allTime;
    public static double perf_algorithmTime;
    public static double perf_createIndexCostTime;
    public static double perf_pluginTime;
    public static double perf_populateTime;

    public static void main(String[] args) throws Exception {
        new SeqWhatIfTest2();
    }
}
