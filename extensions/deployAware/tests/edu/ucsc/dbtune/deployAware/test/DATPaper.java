package edu.ucsc.dbtune.deployAware.test;

import static edu.ucsc.dbtune.DatabaseSystem.newDatabaseSystem;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.io.StringReader;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashSet;
import java.util.Vector;

import edu.ucsc.dbtune.DatabaseSystem;
import edu.ucsc.dbtune.bip.util.LogListener;
import edu.ucsc.dbtune.deployAware.DAT;
import edu.ucsc.dbtune.deployAware.DATBaselines;
import edu.ucsc.dbtune.deployAware.DATOutput;
import edu.ucsc.dbtune.deployAware.DATParameter;
import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.optimizer.DB2Optimizer;
import edu.ucsc.dbtune.optimizer.ExplainedSQLStatement;
import edu.ucsc.dbtune.optimizer.InumOptimizer;
import edu.ucsc.dbtune.seq.bip.SeqInumCost;
import edu.ucsc.dbtune.seq.bip.def.SeqInumIndex;
import edu.ucsc.dbtune.seq.utils.RTimer;
import edu.ucsc.dbtune.seq.utils.RTimerN;
import edu.ucsc.dbtune.util.Environment;
import edu.ucsc.dbtune.util.Rt;
import edu.ucsc.dbtune.util.Rx;
import edu.ucsc.dbtune.workload.Workload;

public class DATPaper {
    public static String[] plotNames = new String[] { "DAT", "DB2", "DA" };
    public static String[] compareNames = new String[] { "DAT/DB2",
            "DAT/GREEDY" };
    SeqInumCost cost;
    double[] windowConstraints;
    double alpha, beta;
    int maxIndexCreatedPerWindow = 0;
    double dat;
    double greedyRatio;
    double mkp;
    DATSeparateProcess dsp;

    public DATPaper(WorkloadLoader loader, GnuPlot2 plot, double plotX, int m,
            long spaceBudge, int l, double alpha, int windowSize, File debugFile)
            throws Exception {
        // m=3;
        // spaceBudge=20*1024L*1024L*1024L;
        // l=10;
        // alpha=0.5;
        Rt.p(plot.name + " " + plot.xName + "=" + plotX);
        cost = loader.loadCost();
        long totalCost = 0;
        for (int i = 0; i < cost.indices.size(); i++) {
            SeqInumIndex index = cost.indices.get(i);
            totalCost += index.createCost;
        }
        // if (windowSize == 0)
        // windowSize = (int) (totalCost / 2 / m);
        // windowSize = (int) (totalCost / 12 / m);
        // cost.indices.size());
        windowConstraints = new double[m];
        if (alpha < 0 || alpha > 1)
            throw new Error();
        this.alpha = alpha;
        this.beta = 1 - alpha;
        for (int i = 0; i < windowConstraints.length; i++)
            windowConstraints[i] = windowSize;
        cost.storageConstraint = spaceBudge;
        maxIndexCreatedPerWindow = l;
        // if (true) {
        dsp = new DATSeparateProcess(loader.dbName, loader.workloadName,
                loader.fileName, loader.generateIndexMethod, alpha, beta, m, l,
                spaceBudge, windowSize, 0);
        dsp.debugFile = debugFile;
        dsp.runMKP = plotNames.length == 3;
        dsp.run();
        dat = dsp.dat;
        mkp = dsp.bip;
        greedyRatio = dsp.greedy;
        // } else {
        // RTimer timer = new RTimer();
        // dat = dat();
        // long time1 = timer.get();
        // greedyRatio = baseline();
        // timer.reset();
        // mkp = mkp();
        // long time2 = timer.get();
        // Rt.p(time1 + " " + time2 + time2 / time1);
        // }
        // plot.setPlotNames(plotNames);
        // plot.add(plotX, dat / greedyRatio * 100);
        // if (dsp.runMKP)
        // plot.add(plotX, dat / mkp * 100);
        // plot.add(plotX, greedyRatio / greedyRatio * 100);
        plot.setPlotNames(compareNames);
        if (dsp.runMKP)
            plot.add(plotX, dat / mkp * 100);
        plot.add(plotX, dat / greedyRatio * 100);
        // plot.add(plotX, dsp.datWindowCosts[0]);
        // plot.add(plotX, dsp.greedyWindowCosts[0]);
        plot.addLine();
        loader.close();
    }

    public static class TestSet {
        public String name;
        public String shortName;
        public String dbName;
        public String workloadName;
        public String fileName;
        public long size;
        public long windowSize;
        public Vector<String> plotNames = new Vector<String>();
        public Vector<String> figureNames = new Vector<String>();

        public TestSet(String name, String dbName, String workloadName,
                long size, String shortName) {
            this(name, dbName, workloadName, "workload.sql", size, shortName, 0);
        }

        public TestSet(String name, String dbName, String workloadName,
                String fileName, long size, String shortName, int windowSize) {
            this.name = name;
            this.dbName = dbName;
            this.workloadName = workloadName;
            this.fileName = fileName;
            this.size = size;
            this.shortName = shortName;
            this.windowSize = windowSize;
        }
    }

    public static double getAlpha(double _1mada) {
        double alpha = 1 / (_1mada + 1);
        if (Math.abs((1 - alpha) / alpha - _1mada) > 1E-5)
            throw new Error();
        return alpha;
    }

    public static void main(String[] args) throws Exception {
        boolean exp = true; // rerun experiment
        boolean scalabilityTest = true;
        exp = false;
        scalabilityTest = false;
        boolean windowOnly = false;
        long rerunTime = 1351755033198L - 3600 * 1000L;
        Rt.p(System.currentTimeMillis());
        // windowOnly = true;
        // generateIndexMethod = "powerset 2";

        // ps.println("\\begin{multicols}{2}\n");
        long gigbytes = 1024L * 1024L * 1024L;
        TestSet[] sets = {
                new TestSet("16 TPC-H queries", "tpch10g", "deployAware",
                        "TPCH16.sql", 10 * gigbytes, "TPCH16", 9 * 3600 * 3000),
                // new TestSet("1 TPC-H queries", "tpch10g", "deployAware",
                // "TPCH1.sql", 10 * gigbytes, "TPCH1", 0),
                // new TestSet("1 TPCDS queries", "test", "deployAware",
                // "TPCDS1.sql", 10 * gigbytes, "TPCDS1"),
                new TestSet("63 TPCDS queries", "test", "deployAware",
                        "TPCDS63.sql", 10 * gigbytes, "TPCDS63", 0),
        // new TestSet("15 TPCDS update queries", "test", "tpcds", "update.sql",
        // 10 * gigbytes, "TPCDSU15"),
        // new TestSet("81 OTAB queries", "test", "deployAware",
        // "OTAB86.sql", 10 * gigbytes, "OTAB86"),
        // new TestSet("TPCDS", "test", "tpcds-inum",
        // gigbytes),
        // new TestSet("22 TPC-H queries", "tpch10g", "tpch",
        // "complete.sql", 10 * gigbytes, "TPC-H"),
        // new TestSet("45 TPCDS queries", "test", "tpcds", "inum.sql",
        // 10 * gigbytes, "TPCDS45"),
        // new
        // TestSet("12 TPC-H queries  \\& update stream RF1 and RF2",
        // "tpch10g", "tpch-benchmark-mix", 10 * gigbytes,
        // "TPC-H update"),
        // new TestSet("25 TPCDS queries", "test", "tpcds-inum",
        // 10 * gigbytes, "TPCDS25"),
        // new TestSet("170 OST queries", "test", "OST", 10 * gigbytes),
        // new TestSet("100 OTAB  queries", "test",
        // "online-benchmark-100", 10 * gigbytes, "OTAB"),
        // new TestSet("100 OTAB queries and 10 updates", "test",
        // "online-benchmark-update-100", 10 * gigbytes,
        // "OTAB update"),
        };
        DATPaperParams params = new DATPaperParams();

        params.windowSize = 9 * 3600 * 3000;// TPCH16
        for (TestSet set : sets) {
            WorkloadLoader loader2 = new WorkloadLoader(set.dbName,
                    set.workloadName, set.fileName, params.generateIndexMethod);
            loader2.getIndexes();
            System.exit(0);
            WorkloadLoader loader = new WorkloadLoader(set.dbName,
                    set.workloadName, set.fileName, params.generateIndexMethod);
            DATSeparateProcess dsp = new DATSeparateProcess(loader.dbName,
                    loader.workloadName, loader.fileName,
                    loader.generateIndexMethod, 1, 1, params.m_def,
                    params.l_def, set.size * params.spaceFactor_def, 0, 0);
            // dsp.run();
        }
        File debugDir = new File("/home/wangrui/dbtune/debug");
        if (false) {
            for (TestSet set : sets) {
                WorkloadLoader loader = new WorkloadLoader(set.dbName,
                        set.workloadName, set.fileName,
                        params.generateIndexMethod);
                SeqInumCost cost = loader.loadCost();
                Rt.np("minCost: %,.0f", cost.costWithAllIndex);
                Rt.np("maxCost: %,.0f", cost.costWithoutIndex);

                GnuPlot2 plot = null;
                long spaceBudge = (long) (set.size * params.spaceFactor_def);
                plot = new GnuPlot2(params.figsDir, set.shortName + "Xdebug",
                        "debug", "debug");
                plot.usePercentage = true;
                plot.setPlotNames(compareNames);
                set.plotNames.add(plot.name);
                set.figureNames.add("debug");
                plot.setXtics(params.spaceFactor_names, params.spaceFactor_set);
                if (exp || !plot.pltFile.exists()) {
                    int m = 3;
                    File debugFile = new File(debugDir, plot.name + "_" + m
                            + ".xml");
                    new DATPaper(loader, plot, m, m, spaceBudge, params.l_def,
                            getAlpha(params._1mada_def), params.windowSize,
                            debugFile);
                }
                // plot.finish();
            }
            System.exit(0);
        }
        for (TestSet set : sets) {
            // SeqInumCost cost = DATTest2.loadCost();
            // long totalCost = 0;
            // for (int i = 0; i < cost.indices.size(); i++) {
            // SeqInumIndex index = cost.indices.get(i);
            // totalCost += index.createCost;
            // }
            Rt.p(set.dbName + " " + set.workloadName + " " + set.fileName);
            WorkloadLoader loader = new WorkloadLoader(set.dbName,
                    set.workloadName, set.fileName, params.generateIndexMethod);
            set.plotNames.clear();
            set.figureNames.clear();
            SeqInumCost cost = loader.loadCost();
            if (windowOnly) {
                Rt.np("minCost: %,.0f", cost.costWithAllIndex);
                Rt.np("maxCost: %,.0f", cost.costWithoutIndex);
            }

            long spaceBudge = (long) (set.size * params.spaceFactor_def);
            GnuPlot2.uniform = true;
            GnuPlot2 plot = null;

            int win = params.m_def;
            plot = new GnuPlot2(params.figsDir, set.shortName + "Xwindow",
                    "window", "cost");
            int[] set2 = new int[win];
            for (int i = 0; i < set2.length; i++)
                set2[i] = i;
            plot.setXtics(set2);
            plot.setPlotNames(plotNames);
            set.plotNames.add(plot.name);
            set.figureNames.add("cost of each window");
            if (exp || !plot.pltFile.exists()
                    || plot.pltFile.lastModified() < rerunTime) {
                File debugFile = new File(debugDir, plot.name + "_"
                        + "window.xml");
                DATPaper run = new DATPaper(loader, plot, win, win, spaceBudge,
                        params.l_def, getAlpha(params._1mada_def),
                        params.windowSize, debugFile);
                plot.vs.clear();
                for (int i = 0; i < win; i++) {
                    plot.add(i, run.dsp.datWindowCosts[i]);
                    if (plotNames.length == 3)
                        plot.add(i, run.dsp.mkpWindowCosts[i]);
                    plot.add(i, run.dsp.greedyWindowCosts[i]);
                    plot.addLine();
                }
            }
            plot.setPlotNames(plotNames);
            plot.finish();

            if (!windowOnly) {
                plot = new GnuPlot2(params.figsDir, set.shortName + "Xm", "m",
                        "cost");
                plot.usePercentage = true;
                plot.setXtics(params.m_set);
                plot.setPlotNames(compareNames);
                set.plotNames.add(plot.name);
                set.figureNames.add("differnt window size");
                if (exp || !plot.pltFile.exists()
                        || plot.pltFile.lastModified() < rerunTime) {
                    for (int m : params.m_set) {
                        File debugFile = new File(debugDir, plot.name + "_" + m
                                + ".xml");
                        new DATPaper(loader, plot, m, m, spaceBudge,
                                params.l_def, getAlpha(params._1mada_def),
                                params.windowSize, debugFile);
                    }
                }
                plot.finish();
                plot = new GnuPlot2(params.figsDir, set.shortName + "Xspace",
                        "space", "cost");
                plot.usePercentage = true;
                plot.setPlotNames(compareNames);
                set.plotNames.add(plot.name);
                set.figureNames.add("different space budget");
                plot.setXtics(params.spaceFactor_names, params.spaceFactor_set);
                if (exp || !plot.pltFile.exists()
                        || plot.pltFile.lastModified() < rerunTime) {
                    int i = 0;
                    for (double spaceFactor : params.spaceFactor_set) {
                        long space = (long) (set.size * spaceFactor);
                        File debugFile = new File(debugDir, plot.name + "_"
                                + params.spaceFactor_names[i] + ".xml");
                        new DATPaper(loader, plot, spaceFactor, params.m_def,
                                space, params.l_def,
                                getAlpha(params._1mada_def), params.windowSize,
                                debugFile);
                        i++;
                    }
                }
                plot.finish();
                plot = new GnuPlot2(params.figsDir, set.shortName + "Xl", "l",
                        "cost");
                plot.usePercentage = true;
                plot.setXtics(params.l_set);
                plot.setPlotNames(compareNames);
                set.plotNames.add(plot.name);
                set.figureNames.add("maximum indexes per window");
                if (exp || !plot.pltFile.exists()
                        || plot.pltFile.lastModified() < rerunTime) {
                    for (int l : params.l_set) {
                        File debugFile = new File(debugDir, plot.name + "_" + l
                                + ".xml");
                        new DATPaper(loader, plot, l, params.m_def, spaceBudge,
                                l, getAlpha(params._1mada_def),
                                params.windowSize, debugFile);
                    }
                }
                plot.finish();
                plot = new GnuPlot2(params.figsDir, set.shortName + "X1mada",
                        "(1-a)/a", "cost");
                plot.usePercentage = true;
                plot.setXtics(params._1mada_set);
                plot.setPlotNames(compareNames);
                set.plotNames.add(plot.name);
                set.figureNames.add("alpha");
                if (exp || !plot.pltFile.exists()
                        || plot.pltFile.lastModified() < rerunTime) {
                    for (int _1mada : params._1mada_set) {
                        double alpha = getAlpha(_1mada);
                        File debugFile = new File(debugDir, plot.name + "_"
                                + _1mada + ".xml");
                        new DATPaper(loader, plot, _1mada, params.m_def,
                                spaceBudge, params.l_def, alpha,
                                params.windowSize, debugFile);
                    }
                }
                plot.finish();
            }

        }

        String windowSize = "cophyRecommendIndexes/m*"
                + DATTest2.windowSizeParameter;
        if (params.windowSize > 0) {
            windowSize = String.format("%,d", params.windowSize);
        }
        if (!windowOnly)
            DATPaperOthers.generatePdf(new File(params.outputDir,
                    params.generateIndexMethod.replace(' ', '_') + ".tex"),
                    params, sets, windowSize);
    }
}
