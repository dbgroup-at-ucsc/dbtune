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
    public static String dbName = "test";
    public static String workloadName = "tpch-500-counts";

    public static Workload getWorkload(Environment en) throws IOException,
            SQLException {
        String file = "/tpch/workload_bip_seq.sql";
        file = "/tpch-small/workload.sql";
        file = "/tpcds-small/workload.sql";
        file = "/tpch-500-counts/workload.sql";
        file = "/" + workloadName + "/workload.sql";
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
                if (querySize != 0 && count >= querySize)
                    break all;
            }
            if (querySize == 0)
                break;
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
        if (size > 0 && indexes.size() >= size) {
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
        File dir = new File("/home/wangrui/workspace/cache/" + dbName + "/"
                + workloadName);
        dir.mkdirs();
        File file = new File(dir, querySize + "_" + indexSize + ".xml");
        SeqInumCost cost = null;
        if (file.exists()) {
            Rt.p("loading from cache " + file.getAbsolutePath());
            Rx rx = Rx.findRoot(Rt.readFile(file));
            cost = SeqInumCost.loadFromXml(rx);
        } else {
            en = Environment.getInstance();
            en.setProperty("jdbc.url", "jdbc:db2://localhost:50000/" + dbName);
            en.setProperty("username", "db2inst1");
            en.setProperty("password", "db2inst1admin");
            Rt.p(en.getProperty("jdbc.url"));
            db = newDatabaseSystem(en);

            Workload workload = DATTest2.getWorkload(en);
            Set<Index> indexes = DATTest2.getIndexes(workload, db);

            InumOptimizer optimizer = (InumOptimizer) db.getOptimizer();
            DB2Optimizer db2optimizer = (DB2Optimizer) optimizer.getDelegate();

            cost = SeqInumCost.fromInum(db, optimizer, workload, indexes);
            for (int i = 0; i < cost.queries.size(); i++) {
                cost.queries.get(i).sql = null;
            }
            for (int i = 0; i < cost.indices.size(); i++) {
                cost.indices.get(i).index = null;
            }

            DAT dat = new DAT(cost, new double[3], 1, 0);
            // dat.setOptimizer(optimizer);
            LogListener logger = LogListener.getInstance();
            dat.setLogListenter(logger);
            dat.setWorkload(new Workload("", new StringReader("")));
            dat.getIndexBenefit();

            Rx root = new Rx("workload");
            cost.save(root);
            String xml = root.getXml();
            Rt.write(file, xml);
        }
        return cost;
    }

    public static void testBIP() throws Exception {
        DAT.invokeCplex();
        SeqCost.totalWhatIfNanoTime = 0;
        SeqCost.totalCreateIndexNanoTime = 0;
        SeqInumCost.populateTime = 0;
        SeqInumCost.plugInTime = 0;
        RTimerN timer = new RTimerN();

        SeqInumCost cost = DATTest2.loadCost();
        double[] windowConstraints = new double[3];
        for (int i = 0; i < windowConstraints.length; i++)
            windowConstraints[i] = 3200000;
        cost.storageConstraint = 16000 * 1024.0 * 1024.0;

        DAT dat = new DAT(cost, windowConstraints, 1, 0);
        // dat.setOptimizer(optimizer);
        LogListener logger = LogListener.getInstance();
        dat.setLogListenter(logger);
        dat.setWorkload(new Workload("", new StringReader("")));
        // dat.getIndexBenefit();

        // File dir = new File("/home/wangrui/workspace/cache", testSet);
        // File file = new File(dir, "tpch100_" + querySize + "_" + indexSize
        // + ".xml");
        // Rx root = new Rx("workload");
        // cost.save(root);
        // String xml = root.getXml();
        // Rt.write(file, xml);

        Rt.np("queryCount=%d \t indexCount=%d\tspace=%,.0fMB", cost.queries
                .size(), cost.indices.size(),
                cost.storageConstraint / 1024 / 1024);
        Rt
                .np("index\tcreate cost\tstorage cost\tbenefit(with-none)\tbenefit(all-without)");
        for (int i = 0; i < cost.indices.size(); i++) {
            SeqInumIndex index = cost.indices.get(i);
            Rt.np("%d\t%,.0f\t%,.0f\t%,.0f\t%,.0f", i, index.createCost,
                    index.storageCost, index.indexBenefit, index.indexBenefit2);
        }
        // DATOutput output = (DATOutput) dat.solve();
        System.out.print("alpha, beta\t");
        for (int i = 0; i < windowConstraints.length; i++) {
            System.out.print("window " + i + "\tcreate/drop\t");
        }
        System.out.println("TransCost");
        System.out.print("windowSize\t");
        for (int i = 0; i < windowConstraints.length; i++) {
            System.out.format("%,.0f\t\t", windowConstraints[i]);
        }
        System.out.println("");
        // DATOutput baseline = (DATOutput) dat.baseline();
        // System.out.print("cophy modified\t");
        // for (int i = 0; i < windowConstraints.length; i++) {
        // System.out.print(baseline.ws[i].cost + "\t");
        // }
        // System.out.println(baseline.totalCost);
        DATOutput baseline = null;
        DATOutput baseline3 = null;
        baseline = (DATOutput) dat.baseline2("greedyRatio");
        System.out.print("greedyRatio MKP\t");
        for (int i = 0; i < windowConstraints.length; i++) {
            System.out.format("%.0f\t\t", baseline.ws[i].cost);
        }
        System.out.format(
                "%.0f\tfit " + DAT.baseline2WindowConstraint + "%%\n",
                baseline.totalCost);
        // baseline2 = (DATOutput) dat.baseline2("greedy");
        // System.out.print("greedy MKP\t");
        // for (int i = 0; i < windowConstraints.length; i++) {
        // System.out.print(baseline2.ws[i].cost + "\t");
        // }
        // System.out.println(baseline2.totalCost+"\t"+
        // DAT.baseline2WindowConstraint+"%");
        // baseline3 = (DATOutput) dat.baseline2("bip");
        // System.out.print("PTAS MKP\t");
        // for (int i = 0; i < windowConstraints.length; i++) {
        // System.out.print(baseline3.ws[i].cost + "\t");
        // }
        // System.out.println(baseline3.totalCost+"\t"+
        // DAT.baseline2WindowConstraint+"%");
        double alpha = 1;
        for (double beta = Math.pow(2, 0); beta <= Math.pow(2, 5); beta *= 2) {
            dat = new DAT(cost, windowConstraints, alpha, beta);
            // dat.setOptimizer(optimizer);
            dat.setLogListenter(logger);
            dat.setWorkload(new Workload("", new StringReader("")));
            dat.buildBIP();
            DATOutput output = (DATOutput) dat.getOutput();
            System.out.print(alpha + ", " + beta + "\t");
            for (int i = 0; i < windowConstraints.length; i++) {
                System.out.format("%.0f\tc " + output.ws[i].create + ", d "
                        + output.ws[i].drop + "\t", output.ws[i].cost);
            }
            System.out.format("%.0f", output.totalCost);
            if (baseline != null) {
                double btotal = baseline.totalCost;
                System.out.format("\t%.0f", btotal);
                System.out.format("\t/base %.0f%%", output.totalCost / btotal
                        * 100);
            }
            // if (baseline2 != null)
            // System.out.print("\t"
            // + (baseline2.totalCost + beta
            // * baseline2.ws[baseline2.ws.length - 1].cost));
            System.out.println();
            dat = new DAT(cost, windowConstraints, alpha, beta);
            baseline3 = (DATOutput) dat.baseline2("bip");
            System.out.print("PTAS MKP\t");
            for (int i = 0; i < windowConstraints.length; i++) {
                System.out.format("%.0f\tc " + baseline3.ws[i].create + "\t",
                        baseline3.ws[i].cost);
                if (baseline3.ws[i].drop != 0)
                    throw new Error();
            }
            double optTotal = baseline3.totalCost;
            System.out.format("%.0f", optTotal);
            System.out.print("\tfit " + DAT.baseline2WindowConstraint + "%");
            System.out.format("\t/opt %.0f%%\n", output.totalCost / optTotal
                    * 100);
        }

        // double datCost = dat.getObjValue();
        // Rt.p("%d x %d", cost.queries.size(), cost.indices.size());
        // Rt.p("cost: %,.0f", dat.getObjValue());
        // Rt.p("time: %.3f", timer.getSecondElapse());
        // output.print();
        // baseline.print();
        if (db != null)
            db.getConnection().close();

        // Rx rx = new Rx("data");
        // rx.createChild("queryCount", cost.queries.size());
        // rx.createChild("indexCount", cost.indices.size());
        // rx.createChild("cost", datCost);
        // Rt.np(rx.getXml().toString());

    }

    public static void testDATBatch() throws Exception {
        DAT.invokeCplex();
        SeqCost.totalWhatIfNanoTime = 0;
        SeqCost.totalCreateIndexNanoTime = 0;
        SeqInumCost.populateTime = 0;
        SeqInumCost.plugInTime = 0;

        SeqInumCost cost = loadCost();

        Rt.np("queries=%d\tindices=%d", cost.queries.size(), cost.indices
                .size());

        double spStart = 500 * 1024.0 * 1024.0;
        double spEnd = 5000000 * 1024.0 * 1024.0;
        double winStart = 100000;
        double winEnd = 500000000;
        winStart = 3200000;
        // spStart=512000* 1024.0 * 1024.0;
        System.out.format("win\\space\t");
        for (double spaceConstraint = spStart; spaceConstraint < spEnd; spaceConstraint *= 2) {
            System.out.format("%,.0fMB\t", spaceConstraint / 1024 / 1024);
        }
        System.out.println();
        for (double winConstraint = winStart; winConstraint < winEnd; winConstraint *= 2) {
            System.out.format("%,.0f\t", winConstraint);
            for (double spaceConstraint = spStart; spaceConstraint < spEnd; spaceConstraint *= 2) {
                double[] windowConstraints = new double[3];
                for (int i = 0; i < windowConstraints.length; i++)
                    windowConstraints[i] = winConstraint;
                cost.storageConstraint = spaceConstraint;

                DAT dat = new DAT(cost, windowConstraints, 1, 0);
                LogListener logger = LogListener.getInstance();
                dat.setLogListenter(logger);
                dat.setWorkload(new Workload("", new StringReader("")));

                DATOutput baseline = null;
                baseline = (DATOutput) dat.baseline2("greedyRatio");
                double fit = DAT.baseline2WindowConstraint;
                double alpha = 1;
                double beta = 1;
                dat = new DAT(cost, windowConstraints, alpha, beta);
                dat.setLogListenter(logger);
                dat.setWorkload(new Workload("", new StringReader("")));
                dat.buildBIP();
                DATOutput output = (DATOutput) dat.getOutput();
                double btotal = (baseline.totalCost + beta
                        * baseline.ws[baseline.ws.length - 1].cost);
                double result = output.totalCost / btotal * 100;
                System.out.format("%.0f%%\t", result);
            }
            System.out.println();
        }
        if (db != null)
            db.getConnection().close();

    }

    public static StringBuilder sb = new StringBuilder();

    public static void main(String[] args) throws Exception {
        dbName = "tpch10g";
        workloadName = "tpch-small";
        querySize = 100;
        indexSize = 200;
        workloadName = "tpch-500-counts";
        // testSet = "online-benchmark-100";
        // testSet = "tpcds-inum";
        // testSet = "tpcds-debug";
        // testSet = "tpch-inserts";
        // testSet = "tpch-deletes";
        // testSet = "tpch-updates";
        // testSet = "nref";
        //
        // querySize = 100;
        // indexSize = 200;
        //
        // querySize = 50;
        // indexSize = 50;
        // querySize = 10;
        // indexSize = 10;
        // querySize = 5;
        // indexSize = 15;
        // querySize = 50;
        // indexSize = 100;

        testBIP();
        // testDATBatch();
    }
}
