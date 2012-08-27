package edu.ucsc.dbtune;

import java.io.File;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import edu.ucsc.dbtune.inum.DerbyInterestingOrdersExtractor;
import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.optimizer.Optimizer;
import edu.ucsc.dbtune.seq.SeqCost;
import edu.ucsc.dbtune.seq.SeqGreedySeq;
import edu.ucsc.dbtune.seq.SeqMerge;
import edu.ucsc.dbtune.seq.SeqOptimal;
import edu.ucsc.dbtune.seq.SeqSplit;
import edu.ucsc.dbtune.seq.def.SeqConfiguration;
import edu.ucsc.dbtune.seq.def.SeqIndex;
import edu.ucsc.dbtune.seq.def.SeqQuery;
import edu.ucsc.dbtune.seq.def.SeqSplitGroup;
import edu.ucsc.dbtune.seq.def.SeqStep;
import edu.ucsc.dbtune.seq.def.SeqStepConf;
import edu.ucsc.dbtune.util.Rt;
import edu.ucsc.dbtune.util.Environment;
import edu.ucsc.dbtune.workload.SQLStatement;

public class SeqFunctionalTest2 {
    SeqCost cost;
    SeqConfiguration[] allConfigurations;

    public SeqFunctionalTest2() throws Exception {
        cost = SeqCost.fromFile(Rt.readResourceAsString(SeqCost.class,
                "disjoint.txt"));
        if (false) {
            Rt.p("Split and merge:");
            // test split and merge
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
            for (SeqIndex[] is : merge.indices) {
                for (SeqIndex index : is) {
                    System.out.format(" "+index.name);
                }
                System.out.println();
            }
        }

        if (true) {
            Rt.p("Greedy heuristic:");
            SeqGreedySeq greedySeq = new SeqGreedySeq(cost, cost.sequence,
                    cost.indicesV.toArray(new SeqIndex[cost.indicesV.size()]));
            while (greedySeq.run())
                ;
            greedySeq.finish();
            Rt.np(SeqOptimal.formatBestPathPlain(greedySeq.bestPath));
        }

        if (true) {
            Rt.p("Optimal algorithm:");
            SeqStep[] steps = SeqOptimal.getOptimalSteps(cost.source,
                    cost.destination, cost.sequence, cost
                            .getAllConfigurations(cost.indicesV));
            SeqOptimal optimal = new SeqOptimal(cost, cost.source,
                    cost.destination, cost.sequence, steps);
            Rt.np(SeqOptimal.formatBestPathPlain(optimal.getBestSteps()));
        }
    }

    public static void main(String[] args) throws Exception {
        new SeqFunctionalTest2();
    }

}
