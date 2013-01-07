package edu.ucsc.dbtune;

import static edu.ucsc.dbtune.util.TestUtils.getBaseOptimizer;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.StringReader;
import java.util.Arrays;
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
        WorkloadLoader loader = new WorkloadLoader("tpch10g", "deployAware",
                "TPCH16.sql", "recommend");
        // loader = new WorkloadLoader("test", "deployAware", "TPCDS63.sql",
        // "recommend");

        int windows = 3;
        cost = SeqCost.multiWindows(loader.loadCost(), windows);
        cost.stepBoost = new double[windows + 1];
        Arrays.fill(cost.stepBoost, 1);
        // cost.stepBoost[2] = 10000;
        // cost = cost.copy(1);
        // cost = cost.dupQuery(500);
        // 1 1.177
        // 2 0.539
        // 10 0.640
        // 100 1.856
        // 500 5.893
        cost.storageConstraint = Double.MAX_VALUE;
        // cost.storageConstraint = 20;
        cost.maxTransitionCost = 50;
        cost.maxIndexes = 10;
        for (SeqIndex index : cost.indicesV)
            index.createCost = 10;
        for (SeqIndex index : cost.indicesV)
            index.storageCost = 10;

        SeqOptimal.noTransitionCost = true;
        SeqGreedySeq greedySeq = new SeqGreedySeq(cost, cost.sequence,
                cost.indicesV.toArray(new SeqIndex[cost.indicesV.size()]));
        while (greedySeq.run())
            ;
        greedySeq.finish();
        if (true)
            Rt.np(SeqOptimal.formatBestPathPlain(greedySeq.bestPath));
        for (int i = 0; i < windows; i++) {
            Rt.p("window " + i + ": "
                    + greedySeq.bestPath[i + 1].queryCost);
        }
        Rt.p("%d x %d", cost.sequence.length, cost.indicesV.size());
        Rt
                .p(
                        "GREEDY cost: %,.0f",
                        greedySeq.bestPath[greedySeq.bestPath.length - 1].costUtilThisStep);
        Rt
                .p(
                        "Obj value: %,.0f",
                        greedySeq.bestPath[greedySeq.bestPath.length - 1].costUtilThisStepBoost);
        perf_cost = greedySeq.bestPath[greedySeq.bestPath.length - 1].costUtilThisStepBoost;
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

        // db.getConnection().close();
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
