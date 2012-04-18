package edu.ucsc.dbtune.deployAware.test;

import java.io.StringReader;

import edu.ucsc.dbtune.bip.util.LogListener;
import edu.ucsc.dbtune.deployAware.DAT;
import edu.ucsc.dbtune.deployAware.DATOutput;
import edu.ucsc.dbtune.seq.SeqCost;
import edu.ucsc.dbtune.seq.SeqGreedySeq;
import edu.ucsc.dbtune.seq.SeqMerge;
import edu.ucsc.dbtune.seq.SeqOptimal;
import edu.ucsc.dbtune.seq.SeqSplit;
import edu.ucsc.dbtune.seq.bip.SebBIPOutput;
import edu.ucsc.dbtune.seq.bip.SeqBIP;
import edu.ucsc.dbtune.seq.bip.SeqInumCost;
import edu.ucsc.dbtune.seq.bip.def.SeqInumIndex;
import edu.ucsc.dbtune.seq.def.SeqConfiguration;
import edu.ucsc.dbtune.seq.def.SeqIndex;
import edu.ucsc.dbtune.seq.def.SeqSplitGroup;
import edu.ucsc.dbtune.seq.def.SeqStep;
import edu.ucsc.dbtune.seq.utils.Rt;
import edu.ucsc.dbtune.workload.Workload;

public class DATFunctionalTest2 {
    SeqConfiguration[] allConfigurations;

    public DATFunctionalTest2() throws Exception {
        if (true) {
            Rt.showDate = false;
            SeqInumCost cost = SeqInumCost.fromFile(Rt.readResourceAsString(
                    DATFunctionalTest2.class, "inum.txt"));
            double[] windowConstraints = new double[] { 1000, 1000, 1000, 1000, };
            DAT dat = new DAT(cost, windowConstraints, 1, 1);
            LogListener logger = LogListener.getInstance();
            dat.setLogListenter(logger);
            dat.setWorkload(new Workload("", new StringReader("")));
            DATOutput output = (DATOutput) dat.solve();
            for (int i = 0; i < output.indexUsed.length; i++) {
                System.out.print("Window " + i + ":");
                for (SeqInumIndex index : output.indexUsed[i]) {
                    System.out.print(" " + index.name);
                }
                System.out.println();
            }
            Rt.p("cost: " + dat.getObjValue());
            System.exit(0);
        }

        SeqCost cost = SeqCost.fromFile(Rt.readResourceAsString(SeqCost.class,
                "optimal.txt"));
        cost.storageConstraint = 10000;

        // if (true) {
        // Rt.p("Greedy heuristic:");
        // SeqGreedySeq greedySeq = new SeqGreedySeq(cost, cost.sequence,
        // cost.indicesV.toArray(new SeqIndex[cost.indicesV.size()]));
        // while (greedySeq.run())
        // ;
        // greedySeq.finish();
        // Rt.np(SeqOptimal.formatBestPathPlain(greedySeq.bestPath));
        // }

        // if (true) {
        // Rt.p("Optimal algorithm:");
        // SeqStep[] steps = SeqOptimal.getOptimalSteps(cost.source,
        // cost.destination, cost.sequence, cost
        // .getAllConfigurations(cost.indicesV));
        // SeqOptimal optimal = new SeqOptimal(cost, cost.source,
        // cost.destination, cost.sequence, steps);
        // Rt.np(SeqOptimal.formatBestPathPlain(optimal.getBestSteps()));
        // }
    }

    public static void main(String[] args) throws Exception {
        new DATFunctionalTest2();
    }

}
