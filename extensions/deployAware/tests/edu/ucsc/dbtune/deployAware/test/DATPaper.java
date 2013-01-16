package edu.ucsc.dbtune.deployAware.test;

import java.io.File;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Vector;

import edu.ucsc.dbtune.DatabaseSystem;
import edu.ucsc.dbtune.deployAware.DAT;
import edu.ucsc.dbtune.deployAware.DATOutput;
import edu.ucsc.dbtune.deployAware.DATParameter;
import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.optimizer.DB2Optimizer;
import edu.ucsc.dbtune.optimizer.ExplainedSQLStatement;
import edu.ucsc.dbtune.seq.SeqCost;
import edu.ucsc.dbtune.seq.SeqGreedySeq;
import edu.ucsc.dbtune.seq.SeqOptimal;
import edu.ucsc.dbtune.seq.bip.SeqInumCost;
import edu.ucsc.dbtune.seq.bip.WorkloadLoader;
import edu.ucsc.dbtune.seq.bip.WorkloadLoaderSettings;
import edu.ucsc.dbtune.seq.bip.def.SeqInumIndex;
import edu.ucsc.dbtune.seq.def.SeqIndex;
import edu.ucsc.dbtune.seq.def.SeqStepConf;
import edu.ucsc.dbtune.seq.utils.RTimerN;
import edu.ucsc.dbtune.util.Rt;

public class DATPaper {
    public static boolean useRatio = false;
    public static boolean addTransitionCostToObjective = false;
    public static boolean eachWindowContainsOneQuery = false;
    public static boolean useDB2Optimizer = false;
    public static boolean verifyByDB2Optimizer = false;
    public static String[] plotNames = new String[] { "DAT", "GREEDY-SEQ"
    // "DB2", "DA"
    };
    public static String[] ratioNames = new String[] { // "DAT/DB2",
    "DAT/GREEDY" };
    public static String[] curNames = useRatio ? ratioNames : plotNames;
    SeqInumCost cost;
    double[] windowConstraints;
    double alpha, beta;
    int maxIndexCreatedPerWindow = 0;
    double dat;
    // double greedyRatio;
    // double mkp;
    double greedySeq;
    double[] datWindowCosts;
    double[] greedyWindowCosts;

    // DATSeparateProcess dsp;

    public DATPaper(WorkloadLoader loader, GnuPlot plot, double plotX, String plotLabel, int m, long spaceBudge, int l,
            double alpha, long windowSize, File debugFile) throws Exception {
        Rt.p(plot.name + " " + plot.xName + "=" + plotX);
        cost = loader.loadCost();
        if (eachWindowContainsOneQuery)
            m = cost.queries.size();
        cost.addTransitionCostToObjective = DATPaper.addTransitionCostToObjective;
        cost.eachWindowContainsOneQuery = DATPaper.eachWindowContainsOneQuery;
        // Rt.p(cost.queries.get(0)
        // .cost(cost.indices.toArray(new SeqInumIndex[0])));
        long totalCost = 0;
        for (int i = 0; i < cost.indices.size(); i++) {
            SeqInumIndex index = cost.indices.get(i);
            totalCost += index.createCost;
        }
        windowConstraints = new double[m];

        if (alpha < 0 || alpha > 1)
            throw new Error();
        this.alpha = alpha;
        this.beta = 1 - alpha;
        for (int i = 0; i < windowConstraints.length; i++)
            windowConstraints[i] = windowSize;
        cost.storageConstraint = spaceBudge;
        maxIndexCreatedPerWindow = l;
        Rt.showDate = false;
        Rt.p("windowSize=%,d m=%d l=%d alpha=%f beta=%f space=%,d", windowSize, m, l, alpha, beta, spaceBudge);

        {
            SeqCost seqCost = eachWindowContainsOneQuery ? SeqCost.fromInum(cost) : SeqCost.multiWindows(cost, m);
            if (useDB2Optimizer) {
                seqCost.useDB2Optimizer = useDB2Optimizer;
                seqCost.db = loader.getDb();
                seqCost.optimizer = loader.getDB2Optimizer();
            }
            seqCost.addTransitionCostToObjective = cost.addTransitionCostToObjective;
            seqCost.stepBoost = new double[m + 1];
            Arrays.fill(seqCost.stepBoost, alpha);
            seqCost.stepBoost[m - 1] = beta; // last
            // window
            seqCost.storageConstraint = spaceBudge;
            seqCost.maxTransitionCost = windowSize;
            seqCost.maxIndexesWindow = l;
            RTimerN timer = new RTimerN();
            SeqGreedySeq greedySeq = new SeqGreedySeq(seqCost);
            while (greedySeq.run());
            greedySeq.finish();
            double totalCost1 = greedySeq.bestPath[greedySeq.bestPath.length - 1].costUtilThisStep;
            double objValue = greedySeq.bestPath[greedySeq.bestPath.length - 1].costUtilThisStepBoost;
            greedyWindowCosts = new double[m];
            this.greedySeq = 0;
            for (int i = 0; i < m; i++) {
                SeqStepConf conf = greedySeq.bestPath[i + 1];
                SeqIndex[] indices = conf.configuration.indices;
                double tc = seqCost.getCost(greedySeq.bestPath[i].configuration, conf.configuration);
                // boolean[] indexUsed = new boolean[seqCost.indicesV.size()];
                // for (SeqIndex index : indices)
                // indexUsed[index.id] = true;
                // DATWindow.costWithIndex(cost, indexUsed);
                greedyWindowCosts[i] = conf.queryCost
                        + (seqCost.addTransitionCostToObjective ? conf.transitionCost : 0);
                Rt.p("GREEDY-SEQ window " + i + ": cost=%,.0f createCost=%,.0f usedIndexes=%d", greedyWindowCosts[i],
                        tc, indices.length);
                if (i < m - 1)
                    this.greedySeq += alpha * greedyWindowCosts[i];
                else
                    this.greedySeq += beta * greedyWindowCosts[i];
            }
            if (!useDB2Optimizer && verifyByDB2Optimizer) {
                Rt.p("verifying window cost");
                for (int i = 0; i < m; i++) {
                    SeqStepConf conf = greedySeq.bestPath[i + 1];
                    DatabaseSystem db = loader.getDb();
                    DB2Optimizer optimizer = loader.getDB2Optimizer();
                    greedyWindowCosts[i] = seqCost.verifyCost(i, db, optimizer, conf)
                            + (seqCost.addTransitionCostToObjective ? conf.transitionCost : 0);
                    Rt.p("GREEDY-SEQ window " + i + ": cost=%,.0f usedIndexes=%d", greedyWindowCosts[i],
                            conf.configuration.indices.length);
                }
            }
            Rt.p("GREEDY-SEQ time: %.2f s", timer.getSecondElapse());
            Rt.p("Obj value: %,.0f", this.greedySeq);
            Rt.p("whatIfCount: %d", seqCost.whatIfCount);
        }

        {
            RTimerN timer = new RTimerN();
            DATParameter params = new DATParameter(cost, windowConstraints, alpha, beta, l);
            DAT dat = new DAT();
            DATOutput output = dat.runDAT(params);
            this.dat = output.totalCost;
            datWindowCosts = new double[m];
            for (int i = 0; i < m; i++) {
                datWindowCosts[i] = output.ws[i].cost;
                Rt.p("DAT Window %d: cost=%,.0f\tcreateCost=%,.0f\tusedIndexes=%d", i, datWindowCosts[i],
                        output.ws[i].createCost, output.ws[i].present);
            }
            if (useDB2Optimizer || verifyByDB2Optimizer) {
                Rt.p("verifying window cost");
                for (int i = 0; i < m; i++) {
                    DatabaseSystem db = loader.getDb();
                    DB2Optimizer optimizer = loader.getDB2Optimizer();
                    datWindowCosts[i] = dat.getWindowCost(i, db, optimizer);
                    Rt.p("DAT Window %d: cost=%,.0f\tcreateCost=%,.0f\tusedIndexes=%d", i, datWindowCosts[i],
                            output.ws[i].createCost, output.ws[i].present);
                }
            }
            // dsp = new DATSeparateProcess(loader.dbName, loader.workloadName,
            // loader.fileName, loader.generateIndexMethod, alpha, beta,
            // m, l, spaceBudge, windowSize, 0);
            // dsp.debugFile = debugFile;
            // dsp.runMKP = plotNames.length == 3;
            // dsp.runMKP = false;
            // dsp.runGreedy = false;
            // dsp.run();
            Rt.p("DAT time: %.2f s", timer.getSecondElapse());
            Rt.p("Obj value: %,.0f", this.dat);
            // dat = dsp.dat;
            // mkp = dsp.bip;
            // greedyRatio = dsp.greedy;
        }

        plot.startNewX(plotX, plotLabel);
        if (useRatio) {
            plot.usePercentage = true;
            plot.addY(dat / greedySeq * 100);
        } else {
            plot.addY(dat);
            plot.addY(greedySeq);
        }
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

        public TestSet(String name, String dbName, String workloadName, long size, String shortName) {
            this(name, dbName, workloadName, "workload.sql", size, shortName, 0);
        }

        public TestSet(String name, String dbName, String workloadName, String fileName, long size, String shortName,
                long windowSize) {
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
        DATPaperParams params = new DATPaperParams();
        boolean exp = true; // rerun experiment
        // exp = false;
        boolean windowOnly = false;
        long tpchWindowSize = 20 * 3600 * 3000;
        long tpcdsWindowSize = 10 * 3600 * 3000;
        // addTransitionCostToObjective = true;
        //        eachWindowContainsOneQuery = true;
//        params.l_def = 10000;
        // useDB2Optimizer = true;
        //         verifyByDB2Optimizer=true;

        // only show window cost with default parameters, skip other
        // experiments.
        windowOnly = true;

        long gigbytes = 1024L * 1024L * 1024L;
        TestSet[] sets = { //
        //        new TestSet("TPCH", "tpch10g", "deployAware", "tpchPaper.sql", 10 * gigbytes, "tpch",//
        //                tpchWindowSize),
//        new TestSet("TPCDS", "test", "deployAware", "tpcdsPaper.sql", 10 * gigbytes, "tpcds", tpcdsWindowSize),
        // new TestSet("63 TPCDS queries", "test", "deployAware",
        // "TPCDS63.sql", 10 * gigbytes, "TPCDS63",
        // tpcdsWindowSize),
                new TestSet("63 TPCDS queries update", "test", "deployAware",
                        "tpcds-update.sql", 10 * gigbytes, "tpcdsupdate",
                        tpcdsWindowSize),
        // new TestSet("15 TPCDS update queries", "test", "tpcds", "update.sql",
        // 10 * gigbytes, "TPCDSU15"),
        // new TestSet("81 OTAB queries", "test", "deployAware",
        // "OTAB86.sql", 10 * gigbytes, "OTAB86"),
        // new TestSet("170 OST queries", "test", "OST", 10 * gigbytes),
        // new TestSet("100 OTAB  queries", "test",
        // "online-benchmark-100", 10 * gigbytes, "OTAB"),
        // new TestSet("100 OTAB queries and 10 updates", "test",
        // "online-benchmark-update-100", 10 * gigbytes,
        // "OTAB update"),
        };
        sets[0].windowSize = Long.MAX_VALUE;

        File debugDir = new File(WorkloadLoaderSettings.dataRoot + "/debug");
        GnuPlot.defaultStyle = GnuPlot.Style.histograms;

        long rerunTime = 0;
        for (TestSet set : sets) {
            if (set.windowSize != 0)
                params.windowSize = set.windowSize;
            Rt.p(set.dbName + " " + set.workloadName + " " + set.fileName);
            WorkloadLoader loader = new WorkloadLoader(set.dbName, set.workloadName, set.fileName,
                    params.generateIndexMethod);
            set.plotNames.clear();
            set.figureNames.clear();
            SeqInumCost cost = loader.loadCost();
            if (windowOnly) {
                Rt.np("minCost: %,.0f", cost.costWithAllIndex);
                Rt.np("maxCost: %,.0f", cost.costWithoutIndex);
            }

            long spaceBudge = (long) (set.size * params.spaceFactor_def);
            GnuPlot.uniform = true;
            GnuPlot plot = null;

            int win = params.m_def;
            plot = new GnuPlot(params.figsDir, set.shortName + "Xwindow", "window", "cost");
            // int[] set2 = new int[win];
            // for (int i = 0; i < set2.length; i++)
            // set2[i] = i;
            // plot.setXtics(set2);
            plot.setPlotNames(plotNames);
            set.plotNames.add(plot.name);
            set.figureNames.add("cost of each window");
            if (exp || !plot.pltFile.exists() || plot.pltFile.lastModified() < rerunTime) {
                File debugFile = new File(debugDir, plot.name + "_" + "window.xml");
                DATPaper run = new DATPaper(loader, plot, win, null, win, spaceBudge, params.l_def,
                        getAlpha(params._1mada_def), params.windowSize, debugFile);
                plot.vs.clear();
                plot.current.clear();
                plot.xtics.clear();
                for (int i = 0; i < win; i++) {
                    plot.startNewX(i);
                    plot.addY(run.datWindowCosts[i]);
                    // if (plotNames.length == 3)
                    // plot.addY(run.mkpWindowCosts[i]);
                    plot.addY(run.greedyWindowCosts[i]);
                }
            }
            plot.setPlotNames(plotNames);
            plot.finish();

            if (!windowOnly) {
                plot = new GnuPlot(params.figsDir, set.shortName + "Xm", "m", "cost");
                // plot.setXtics(params.m_set);
                plot.setPlotNames(curNames);
                set.plotNames.add(plot.name);
                set.figureNames.add("differnt window size");
                if (exp || !plot.pltFile.exists() || plot.pltFile.lastModified() < rerunTime) {
                    for (int m : params.m_set) {
                        File debugFile = new File(debugDir, plot.name + "_" + m + ".xml");
                        new DATPaper(loader, plot, m, null, m, spaceBudge, params.l_def, getAlpha(params._1mada_def),
                                params.windowSize, debugFile);
                    }
                }
                plot.finish();
                plot = new GnuPlot(params.figsDir, set.shortName + "Xspace", "space", "cost");
                plot.setPlotNames(curNames);
                set.plotNames.add(plot.name);
                set.figureNames.add("different space budget");
                // plot.setXtics(params.spaceFactor_names,
                // params.spaceFactor_set);
                if (exp || !plot.pltFile.exists() || plot.pltFile.lastModified() < rerunTime) {
                    int i = 0;
                    for (int k = 0; k < params.spaceFactor_set.length; k++) {
                        double spaceFactor = params.spaceFactor_set[k];
                        long space = (long) (set.size * spaceFactor);
                        File debugFile = new File(debugDir, plot.name + "_" + params.spaceFactor_names[i] + ".xml");
                        new DATPaper(loader, plot, spaceFactor, params.spaceFactor_names[k], params.m_def, space,
                                params.l_def, getAlpha(params._1mada_def), params.windowSize, debugFile);
                        i++;
                    }
                }
                plot.finish();
                plot = new GnuPlot(params.figsDir, set.shortName + "Xl", "l", "cost");
                // plot.setXtics(params.l_set);
                plot.setPlotNames(curNames);
                set.plotNames.add(plot.name);
                set.figureNames.add("maximum indexes per window");
                if (exp || !plot.pltFile.exists() || plot.pltFile.lastModified() < rerunTime) {
                    for (int l : params.l_set) {
                        File debugFile = new File(debugDir, plot.name + "_" + l + ".xml");
                        new DATPaper(loader, plot, l, null, params.m_def, spaceBudge, l, getAlpha(params._1mada_def),
                                params.windowSize, debugFile);
                    }
                }
                plot.finish();
                plot = new GnuPlot(params.figsDir, set.shortName + "X1mada", "(1-a)/a", "cost");
                // plot.setXtics(params._1mada_set);
                plot.setPlotNames(curNames);
                set.plotNames.add(plot.name);
                set.figureNames.add("alpha");
                if (exp || !plot.pltFile.exists() || plot.pltFile.lastModified() < rerunTime) {
                    for (int _1mada : params._1mada_set) {
                        double alpha = getAlpha(_1mada);
                        File debugFile = new File(debugDir, plot.name + "_" + _1mada + ".xml");
                        new DATPaper(loader, plot, _1mada, null, params.m_def, spaceBudge, params.l_def, alpha,
                                params.windowSize, debugFile);
                    }
                }
                plot.finish();
            }

        }

        String windowSize = "cophyRecommendIndexes/m*" + DATTest2.windowSizeParameter;
        if (params.windowSize > 0) {
            windowSize = String.format("%,d", params.windowSize);
        }
        if (!windowOnly)
            DATPaperOthers.generatePdf(
                    new File(params.outputDir, params.generateIndexMethod.replace(' ', '_') + ".tex"), params, sets,
                    windowSize);
    }
}
