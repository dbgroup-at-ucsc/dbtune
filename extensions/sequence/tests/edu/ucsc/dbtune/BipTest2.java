package edu.ucsc.dbtune;

import java.io.File;

import edu.ucsc.dbtune.seq.SeqCost;
import edu.ucsc.dbtune.seq.bip.SebBIPOutput;
import edu.ucsc.dbtune.seq.bip.SeqBIP;
import edu.ucsc.dbtune.seq.bip.SeqInumCost;
import edu.ucsc.dbtune.seq.bip.WorkloadLoader;
import edu.ucsc.dbtune.seq.bip.def.SeqInumIndex;
import edu.ucsc.dbtune.util.Rt;
import edu.ucsc.dbtune.util.Rx;

public class BipTest2 {
    public static void testBIP() throws Exception {
        WorkloadLoader loader = new WorkloadLoader("tpch10g", "deployAware",
                "TPCH16.sql", "recommend");
        SeqInumCost cost = loader.loadCost();
        Rt.p("queries: %d  indexes: %d", cost.queries.size(), cost.indices
                .size());
        cost.storageConstraint = Double.MAX_VALUE;// 2000;
        for (int i = 0; i < cost.indices.size(); i++)
            cost.indices.get(i).storageCost = 1000;
        SeqBIP bip = new SeqBIP(cost);
        SebBIPOutput output = (SebBIPOutput) bip.solve();
        double bipCost = bip.getObjValue();
        Rt.p("%d x %d", cost.queries.size(), cost.indices.size());
        Rt.p("cost: %,.0f", bip.getObjValue());
        for (int i = 0; i < output.indexUsed.length; i++) {
            System.out.print("Query " + i + ":");
            for (SeqInumIndex index : output.indexUsed[i]) {
                System.out.print(" " + index.name);
            }
            System.out.println();
        }

        Rx rx = new Rx("data");
        rx.createChild("queryCount", cost.queries.size());
        rx.createChild("indexCount", cost.indices.size());
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
    }

    public static StringBuilder sb = new StringBuilder();

    public static void main(String[] args) throws Exception {
        args = new String[] { "1", "100", "greedy" };
        if ("bip".equals(args[2]))
            testBIP();
        else if ("greedy".equals(args[2]))
            testGREEDY();
        else
            throw new Error();
    }
}
