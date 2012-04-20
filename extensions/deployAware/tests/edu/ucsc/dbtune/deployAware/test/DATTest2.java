package edu.ucsc.dbtune.deployAware.test;

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
import edu.ucsc.dbtune.deployAware.DAT;
import edu.ucsc.dbtune.deployAware.DATOutput;
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
import edu.ucsc.dbtune.seq.utils.Rx;
import edu.ucsc.dbtune.util.Environment;
import edu.ucsc.dbtune.workload.Workload;

import org.junit.BeforeClass;
import org.junit.Test;

import static edu.ucsc.dbtune.DatabaseSystem.newDatabaseSystem;
import static edu.ucsc.dbtune.bip.sim.SimVariablePool.VAR_X;
import static edu.ucsc.dbtune.bip.sim.SimVariablePool.VAR_Y;
import static edu.ucsc.dbtune.util.TestUtils.workload;
import static edu.ucsc.dbtune.util.TestUtils.getBaseOptimizer;

public class DATTest2 {
    public static DatabaseSystem db;
    public static Environment en;
    public static int querySize = 20;
    public static int indexSize = 10;
    public static String testSet = "tpch-500-counts";

    public static Workload getWorkload(Environment en) throws IOException,
            SQLException {
        String file = "/tpch/workload_bip_seq.sql";
        file = "/tpch-small/workload.sql";
        file = "/tpcds-small/workload.sql";
        file = "/tpch-500-counts/workload.sql";
        file = "/" + testSet + "/workload.sql";
        Workload workload = new Workload("", new FileReader(en
                .getWorkloadsFoldername()
                + file));
        Rt.np("workload size: " + workload.size());
        StringBuilder sb = new StringBuilder();
        int count = 0;
        all: while (true) {
            for (int i = 0; i < workload.size(); i++) {
                sb.append(workload.get(i).getSQL() + ";\r\n");
                count++;
                if (count >= querySize)
                    break all;
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
            Rt.np("Reduce index size from " + indexes.size() + " to " + size);
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

    public static SeqInumCost loadCost() throws Exception {
        File dir = new File("/home/wangrui/workspace/cache", testSet);
        dir.mkdir();
        File file = new File(dir, "tpch100_" + querySize + "_" + indexSize
                + ".xml");
        SeqInumCost cost = null;
        if (file.exists()) {
            Rt.p("loading from cache " + file.getAbsolutePath());
            Rx rx = Rx.findRoot(Rt.readFile(file));
            cost = SeqInumCost.loadFromXml(rx);
        } else {
            en = Environment.getInstance();
            db = newDatabaseSystem(en);

            Workload workload = DATTest2.getWorkload(en);
            Set<Index> indexes = DATTest2.getIndexes(workload, db);

            InumOptimizer optimizer = (InumOptimizer) db.getOptimizer();
            DB2Optimizer db2optimizer = (DB2Optimizer) optimizer.getDelegate();

            cost = SeqInumCost.fromInum(optimizer, workload, indexes);
            for (int i = 0; i < cost.queries.size(); i++) {
                cost.queries.get(i).sql = null;
            }
            for (int i = 0; i < cost.indices.size(); i++) {
                cost.indices.get(i).index = null;
            }
            Rx root = new Rx("workload");
            cost.save(root);
            String xml = root.getXml();
            Rt.write(file, xml);
        }
        return cost;
    }

    public static void testBIP() throws Exception {
        SeqCost.totalWhatIfNanoTime = 0;
        SeqCost.totalCreateIndexNanoTime = 0;
        SeqInumCost.populateTime = 0;
        SeqInumCost.plugInTime = 0;
        RTimerN timer = new RTimerN();

        SeqInumCost cost = loadCost();
        Rt.p("%d x %d", cost.queries.size(), cost.indices.size());
        Rt.np("Create index cost");
        for (int i = 0; i < cost.indices.size(); i++) {
            Rt.np("%,.0f", cost.indices.get(i).createCost);
        }
        double[] windowConstraints = new double[10];
        for (int i = 0; i < windowConstraints.length; i++)
            windowConstraints[i] = 1000000;
        DAT dat = new DAT(cost, windowConstraints, 1, 1);
        // dat.setOptimizer(optimizer);
        LogListener logger = LogListener.getInstance();
        dat.setLogListenter(logger);
        dat.setWorkload(new Workload("", new StringReader("")));
        // DATOutput output = (DATOutput) dat.solve();
        System.out.print("alpha, beta\t");
        for (int i = 0; i < windowConstraints.length; i++) {
            System.out.print("window "+i + "\t");
        }
        System.out.println("TransCost");
        System.out.print("constraint\t");
        for (int i = 0; i < windowConstraints.length; i++) {
            System.out.print(windowConstraints[i] + "\t");
        }
        System.out.println("");
        DATOutput baseline = (DATOutput) dat.baseline();
        System.out.print("cophy\t");
        for (int i = 0; i < windowConstraints.length; i++) {
            System.out.print(baseline.ws[i].cost + "\t");
        }
        System.out.println(baseline.totalCost);
        double alpha=1;
        for (double belta = Math.pow(2,-7); belta <= Math.pow(2, 7); belta *= 2) {
            dat = new DAT(cost, windowConstraints, alpha, belta);
            // dat.setOptimizer(optimizer);
            dat.setLogListenter(logger);
            dat.setWorkload(new Workload("", new StringReader("")));
            DATOutput output = (DATOutput) dat.solve();
            System.out.print(alpha+", "+ belta + "\t");
            for (int i = 0; i < windowConstraints.length; i++) {
                System.out.print(output.ws[i].cost + "\t");
            }
            System.out.println(output.totalCost);
        }

        double datCost = dat.getObjValue();
        Rt.p("%d x %d", cost.queries.size(), cost.indices.size());
        Rt.p("cost: %,.0f", dat.getObjValue());
        Rt.p("time: %.3f", timer.getSecondElapse());
        // output.print();
        // baseline.print();
        if (db != null)
            db.getConnection().close();

        Rx rx = new Rx("data");
        rx.createChild("queryCount", cost.queries.size());
        rx.createChild("indexCount", cost.indices.size());
        rx.createChild("cost", datCost);
        Rt.np(rx.getXml().toString());

    }

    public static StringBuilder sb = new StringBuilder();

    public static void main(String[] args) throws Exception {
        testSet = "tpch-small";
        querySize = 10;
        indexSize = 200;
//        testSet = "tpch-500-counts";
//
//        querySize = 100;
//        indexSize = 200;
//
//        querySize = 50;
//        indexSize = 50;

        testBIP();
    }
}
