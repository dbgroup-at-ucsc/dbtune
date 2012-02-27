package edu.ucsc.dbtune.seq;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import edu.ucsc.dbtune.DatabaseSystem;
import edu.ucsc.dbtune.inum.DerbyInterestingOrdersExtractor;
import edu.ucsc.dbtune.metadata.Catalog;
import edu.ucsc.dbtune.metadata.Column;
import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.metadata.Schema;
import edu.ucsc.dbtune.metadata.Table;
import edu.ucsc.dbtune.optimizer.Optimizer;
import edu.ucsc.dbtune.seq.def.*;
import edu.ucsc.dbtune.seq.utils.Rt;
import edu.ucsc.dbtune.util.Environment;
import edu.ucsc.dbtune.workload.SQLStatement;

public class SeqExperiment {
    SeqCost cost;
    SeqConfiguration[] allConfigurations;

    public SeqExperiment() throws Exception {
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
        Environment en = Environment.getInstance();
        en.setProperty("optimizer", "dbms");
        DatabaseSystem db = DatabaseSystem.newDatabaseSystem(en);
        Optimizer optimizer = db.getOptimizer();
        String workloadFile = en
                .getScriptAtWorkloadsFolder("tpch/workload_seq.sql");
        String text = Rt.readFile(new File(workloadFile));
        text = text.substring(0, text.indexOf("exit;"));
        text = text.replaceAll("--.*\n", "");

        List<Set<Index>> indexesPerTable;
        DerbyInterestingOrdersExtractor interestingOrdersExtractor;

        interestingOrdersExtractor = new DerbyInterestingOrdersExtractor(db
                .getCatalog(), true);
        indexesPerTable = interestingOrdersExtractor
                .extract(new SQLStatement(
                        "select\n"
                                + "            n_name as nation,\n"
                                + "            year(o_orderdate) as o_year,\n"
                                + "            l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity as amount\n"
                                + "        from\n" + "            tpch.part,\n"
                                + "            tpch.supplier,\n"
                                + "            tpch.lineitem,\n"
                                + "            tpch.partsupp,\n"
                                + "            tpch.orders,\n"
                                + "            tpch.nation\n"
                                + "        where\n"
                                + "            s_suppkey = l_suppkey\n"
                                + "            and ps_suppkey = l_suppkey\n"
                                + "            and ps_partkey = l_partkey\n"
                                + "            and p_partkey = l_partkey\n"
                                + "            and o_orderkey = l_orderkey\n"
                                + "            and s_nationkey = n_nationkey\n"
                                + "            and p_name like '%thistle%'"));
        HashSet<Index> allIndex = new HashSet<Index>();
        for (Set<Index> set : indexesPerTable)
            for (Index index : set)
                allIndex.add(index);

        Index[] indices = allIndex.toArray(new Index[allIndex.size()]);
        cost = SeqCost.fromOptimizer(db, optimizer, text.split(";\r?\n?"),
                indices);
        int n=0;
        for (SeqQuery query : cost.sequence) {
            System.out.println((n++)+" "+query);
        }
        n=0;
        for (SeqIndex index : cost.indicesV) {
            System.out.println((n++)+" "+index);
        }

        long startTime = System.currentTimeMillis();
        SeqGreedySeq greedySeq = new SeqGreedySeq(cost, cost.sequence,
                cost.indicesV.toArray(new SeqIndex[cost.indicesV.size()]));
        while (greedySeq.run())
            ;
        greedySeq.finish();
        Rt.np(SeqOptimal.formatBestPathPlain(greedySeq.bestPath));
        Rt.np("%fs", (System.currentTimeMillis() - startTime) / 1000.0);
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

        db.getConnection().close();
    }

    public static void main(String[] args) throws Exception {
        new SeqExperiment();
    }
}
