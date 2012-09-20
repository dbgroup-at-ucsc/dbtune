package edu.ucsc.dbtune.deployAware.test;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintStream;
import java.io.StringReader;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Properties;
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
import edu.ucsc.dbtune.deployAware.CPlexWrapper;
import edu.ucsc.dbtune.deployAware.DAT;
import edu.ucsc.dbtune.deployAware.DATBaselines;
import edu.ucsc.dbtune.deployAware.DATOutput;
import edu.ucsc.dbtune.deployAware.DATParameter;
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
import edu.ucsc.dbtune.seq.utils.PerfTest;
import edu.ucsc.dbtune.seq.utils.RTimer;
import edu.ucsc.dbtune.seq.utils.RTimerN;
import edu.ucsc.dbtune.util.Rt;
import edu.ucsc.dbtune.util.Rx;
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
    public static void testBIP(WorkloadLoader loader) throws Exception {
        CPlexWrapper.invokeCplex();
        SeqCost.totalWhatIfNanoTime = 0;
        SeqCost.totalCreateIndexNanoTime = 0;
        SeqInumCost.populateTime = 0;
        SeqInumCost.plugInTime = 0;
        RTimerN timer = new RTimerN();

        SeqInumCost cost = loader.loadCost();

        long totalCost = 0;
        for (int i = 0; i < cost.indices.size(); i++) {
            SeqInumIndex index = cost.indices.get(i);
            totalCost += index.createCost;
        }
        int l = 6;
        int windowSize = 5 * l * (int) (totalCost / cost.indices.size());
        windowSize = 1200;

        double[] windowConstraints = new double[2];
        for (int i = 0; i < windowConstraints.length; i++)
            windowConstraints[i] = windowSize;
        cost.storageConstraint = 5 * 1024 * 1024.0 * 1024.0;

        DATParameter params = new DATParameter(cost, windowConstraints, 1, 0, 0);
        params.maxIndexCreatedPerWindow = l;
        // dat.setOptimizer(optimizer);
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
        baseline = (DATOutput) DATBaselines.baseline2(params, "greedyRatio",
                null);
        System.out.print("greedyRatio MKP\t");
        for (int i = 0; i < windowConstraints.length; i++) {
            System.out.format("%.0f\t\t", baseline.ws[i].cost);
        }
        System.out.format("%.0f\tfit " + DATBaselines.baseline2WindowConstraint
                + "%%\n", baseline.totalCost + baseline.last());
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
            alpha = 1.0 / 3;
            beta = 2.0 / 3;
            DATOutput output = new DAT().runDAT(params);
            System.out.print(alpha + ", " + beta + "\t");
            for (int i = 0; i < windowConstraints.length; i++) {
                System.out.format("%.0f\tc " + output.ws[i].create + ", d "
                        + output.ws[i].drop + "\t", output.ws[i].cost);
            }
            System.out.format("%.0f", output.totalCost);
            if (baseline != null) {
                double btotal = alpha * baseline.totalCost + baseline.last()
                        * beta;
                System.out.format("\t%.0f", btotal);
                System.out.format("\t/base %.0f%%", output.totalCost / btotal
                        * 100);
            }
            // if (baseline2 != null)
            // System.out.print("\t"
            // + (baseline2.totalCost + beta
            // * baseline2.ws[baseline2.ws.length - 1].cost));
            System.out.println();
            baseline3 = (DATOutput) DATBaselines.baseline2(params, "bip", null);
            System.out.print("PTAS MKP\t");
            for (int i = 0; i < windowConstraints.length; i++) {
                System.out.format("%.0f\tc " + baseline3.ws[i].create + "\t",
                        baseline3.ws[i].cost);
                if (baseline3.ws[i].drop != 0)
                    throw new Error();
            }
            double optTotal = baseline3.totalCost;
            System.out.format("%.0f", optTotal);
            System.out.print("\tfit " + DATBaselines.baseline2WindowConstraint
                    + "%");
            System.out.format("\t/opt %.0f%%\n", output.totalCost / optTotal
                    * 100);
            break;
        }

        // double datCost = dat.getObjValue();
        // Rt.p("%d x %d", cost.queries.size(), cost.indices.size());
        // Rt.p("cost: %,.0f", dat.getObjValue());
        // Rt.p("time: %.3f", timer.getSecondElapse());
        // output.print();
        // baseline.print();
        loader.close();
        // Rx rx = new Rx("data");
        // rx.createChild("queryCount", cost.queries.size());
        // rx.createChild("indexCount", cost.indices.size());
        // rx.createChild("cost", datCost);
        // Rt.np(rx.getXml().toString());

    }

    public static void testDATBatch(WorkloadLoader loader) throws Exception {
        CPlexWrapper.invokeCplex();
        SeqCost.totalWhatIfNanoTime = 0;
        SeqCost.totalCreateIndexNanoTime = 0;
        SeqInumCost.populateTime = 0;
        SeqInumCost.plugInTime = 0;

        SeqInumCost cost = loader.loadCost();

        Rt.np("queries=%d\tindices=%d", cost.queries.size(), cost.indices
                .size());

        double spStart = 500 * 1024.0 * 1024.0;
        double spEnd = 5000000 * 1024.0 * 1024.0;
        double winStart = 100000;
        double winEnd = 500000000;
        winStart = 25600000;
        winEnd = 51200000;
        spStart = 16000000000L;
        spEnd = 512000000000L;
        // spStart=512000* 1024.0 * 1024.0;
        System.out.format("win\\space\t");
        for (double spaceConstraint = spStart; spaceConstraint < spEnd; spaceConstraint *= 2) {
            System.out.format("%,.0fMB\t", spaceConstraint / 1024 / 1024);
        }
        System.out.println();
        for (double winConstraint = winStart; winConstraint <= winEnd; winConstraint *= 2) {
            System.out.format("%,.0f\t", winConstraint);
            for (double spaceConstraint = spStart; spaceConstraint < spEnd; spaceConstraint *= 2) {
                double[] windowConstraints = new double[3];
                for (int i = 0; i < windowConstraints.length; i++)
                    windowConstraints[i] = winConstraint;
                cost.storageConstraint = spaceConstraint;

                DATParameter params = new DATParameter(cost, windowConstraints,
                        1, 1, 0);
                DATOutput baseline = null;
                baseline = (DATOutput) DATBaselines.baseline2(params, "bip",
                        null);
                double fit = DATBaselines.baseline2WindowConstraint;
                double alpha = 1;
                double beta = 1;
                params = new DATParameter(cost, windowConstraints, alpha, beta,
                        0);
                DATOutput output = new DAT().runDAT(params);
                double btotal = baseline.totalCost;
                double result = output.totalCost / btotal * 100;
                System.out.format("%.0f%%\t", result);
            }
            System.out.println();
        }
        loader.close();
    }

    public static StringBuilder sb = new StringBuilder();

    static void batch(String[] args) throws Exception {
        int pos = 0;
        Rx input = Rx.findRoot(Rt.readFile(new File(args[pos++])));
        String dbName = input.getChildText("dbName");
        String workloadName = input.getChildText("workloadName");
        String fileName = input.getChildText("fileName");
        String generateIndexMethod = input.getChildText("generateIndexMethod");
        WorkloadLoader loader = new WorkloadLoader(dbName, workloadName,
                fileName, generateIndexMethod);
        double alpha = input.getChildDoubleContent("alpha");
        double beta = input.getChildDoubleContent("beta");
        int m = input.getChildIntContent("m");
        int l = input.getChildIntContent("l");
        double space = input.getChildDoubleContent("spaceBudge");
        double windowSize = input.getChildDoubleContent("windowSize");
        double intermediateConstraint = input
                .getChildDoubleContent("intermediateConstraint");
        String perfReportFile = input.getChildText("perfReportFile");
        String debugFile = input.getChildText("debugFile");
        boolean runDAT = input.getChildBooleanContent("runDAT", true);
        boolean runGreedy = input.getChildBooleanContent("runGreedy", true);
        boolean runMKP = input.getChildBooleanContent("runMKP", true);
        int dupWorkloadNTimes = input.getChildIntContent("dupWorkloadNTimes");
        File outputFile = new File(args[pos++]);

        SeqInumCost cost = loader.loadCost();
        if (dupWorkloadNTimes > 1)
            cost = cost.dup(dupWorkloadNTimes);

        Rt.np("queryCount=%d\tindexCount", cost.queries.size(), cost.indices
                .size());
        Rt.np("alpha=%.2f\tbeta=%.2f", alpha, beta);
        Rt.np("m=%d\tl=%d", m, l);
        Rt.np("window=%.2f\tspace=%.2f", windowSize, space);
        Rt.np("outputFile=" + outputFile.getAbsolutePath());
        Rt.np("queries=%d\tindices=%d", cost.queries.size(), cost.indices
                .size());

        double[] windowConstraints = new double[m];
        for (int i = 0; i < windowConstraints.length; i++)
            windowConstraints[i] = windowSize;
        cost.storageConstraint = space;

        Rx root = new Rx("DAT");
        RTimer timer = new RTimer();
        DATParameter params = new DATParameter(cost, windowConstraints, alpha,
                beta, l);
        params.intermediateConstraint = intermediateConstraint;
        if (Math.abs(windowSize) < 0.01) {
            double total = 0;
            boolean[] indexPresent = DATBaselines.cophy(params,
                    Double.MAX_VALUE, Double.MAX_VALUE, m * l);
            for (int i = 0; i < indexPresent.length; i++) {
                if (indexPresent[i]) {
                    Rt.p(params.costModel.indices.get(i));
                    total += params.costModel.indices.get(i).createCost;
                }
            }
            windowSize = total / m/2;
            Arrays.fill(params.windowConstraints, windowSize);
            Rt.p("total=%,.0f m=%d", total, m);
            Rt
                    .p("maxIndexCreatedPerWindow=%d",
                            params.maxIndexCreatedPerWindow);
            Rt.p("windowSize=%,.0f", windowSize);
        }
        double datCost = 0;
        Rx debug = null;
        if (debugFile != null) {
            debug = new Rx("workload");
            Rx dataset = debug.createChild("dataset");
            dataset.createChild("database", dbName);
            dataset.createChild("workloadName", workloadName);
            dataset.createChild("generateIndexMethod", generateIndexMethod);
            dataset.createChild("alpha", alpha);
            dataset.createChild("beta", beta);
            dataset.createChild("m", m);
            dataset.createChild("l", l);
            dataset.createChild("space", space);
            dataset.createChild("windowSize", windowSize);
            dataset.createChild("intermediateConstraint",
                    intermediateConstraint);
            debug.setAttribute("costWithoutIndex",
                    params.costModel.costWithoutIndex);
            debug.setAttribute("costWithAllIndex",
                    params.costModel.costWithAllIndex);
            Rx q = debug.createChild("query");
            Rx i = debug.createChild("index");
            for (SeqInumIndex index : params.costModel.indices)
                index.save(i.createChild("index"));
            for (SeqInumQuery query : params.costModel.queries)
                query.save(q.createChild("query"));
        }
        if (runDAT) {
            try {
                DAT dat = new DAT();
                dat.debug = debug;
                DATOutput output = dat.runDAT(params);
                double d = 0;
                for (int i = 0; i < windowConstraints.length - 1; i++) {
                    d += output.ws[i].cost;
                }
                datCost = output.totalCost;
                root.createChild("dat", output.totalCost);
                root.createChild("datIntermediate", d);
                Rx windows = root.createChild("datWindows");
                for (int i = 0; i < windowConstraints.length; i++) {
                    Rx window = windows.createChild("window");
                    window.setAttribute("id", i);
                    window.setAttribute("cost", output.ws[i].cost);
                    window.setAttribute("create", output.ws[i].create);
                    window.setAttribute("drop", output.ws[i].drop);
                }
            } catch (Error e) {
                if ("Can't solve bip".equals(e.getMessage())) {
                    root.createChild("dat", 0);
                    root.createChild("datIntermediate", 0);
                } else {
                    e.printStackTrace();
                }
            }
        }
        // PrintStream ps=new PrintStream(new FileOutputStream(new
        // File("/home/wangrui/dbtune/createDrop.txt"),true));
        // for (int i = 0; i < windowConstraints.length; i++) {
        // ps.format("%.0f\tc " + output.ws[i].create + ", d "
        // + output.ws[i].drop + "\t", output.ws[i].cost);
        // }
        // ps.println();
        // ps.close();
        long time1 = timer.get();

        if (runGreedy) {
            DATOutput greedyRatio = (DATOutput) DATBaselines.baseline2(params,
                    "greedyRatio", debug);
            root.createChild("greedyRatio", greedyRatio.totalCost);
            Rx windows = root.createChild("greedyWindows");
            for (int i = 0; i < windowConstraints.length; i++) {
                Rx window = windows.createChild("window");
                window.setAttribute("id", i);
                window.setAttribute("cost", greedyRatio.ws[i].cost);
                window.setAttribute("create", greedyRatio.ws[i].create);
                window.setAttribute("drop", greedyRatio.ws[i].drop);
            }
        }
        timer.reset();

        double bipCost = 0;
        if (runMKP) {
            DATOutput bip = (DATOutput) DATBaselines.baseline2(params, "bip",
                    debug);
            bipCost = bip.totalCost;
            root.createChild("bip", bipCost);
            Rx windows = root.createChild("mkpWindows");
            for (int i = 0; i < windowConstraints.length; i++) {
                Rx window = windows.createChild("window");
                window.setAttribute("id", i);
                window.setAttribute("cost", bip.ws[i].cost);
                window.setAttribute("create", bip.ws[i].create);
                window.setAttribute("drop", bip.ws[i].drop);
            }
        }
        long time2 = timer.get();
        Rt.p("TIME " + time1 + " " + time2 + " " + (double) time2 / time1);

        Rt.p(datCost);
        Rt.p(bipCost);
        double result = datCost / bipCost * 100;
        Rt.p(result + "%");

        Rt.write(outputFile, root.getXml());
        if (perfReportFile != null)
            PerfTest.report(new File(perfReportFile));
        if (debugFile != null) {
            new File(debugFile).getParentFile().mkdirs();
            Rt.write(new File(debugFile), debug.getXml().getBytes());
        }
        loader.close();
        System.exit(0);
    }

    public static void main(String[] args) throws Exception {
        if (args.length == 2)
            batch(args);
        StringBuilder sb = new StringBuilder();
        sb.append("/home/wangrui/dbtune/tmpInput.txt");
        sb.append(" /home/wangrui/dbtune/tmp.txt");
        batch(sb.toString().split(" "));

        String dbName = "tpch10g";
        String workloadName = "tpch-inum";
        dbName = "test";
        workloadName = "online-benchmark-100";
        String generateIndexMethod = "recommend";
        WorkloadLoader loader = new WorkloadLoader(dbName, workloadName,
                "workload.sql", generateIndexMethod);
        testDATBatch(loader);
    }
}
