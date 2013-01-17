package edu.ucsc.dbtune.deployAware.test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.PrintStream;
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
import edu.ucsc.dbtune.seq.bip.def.SeqInumQuery;
import edu.ucsc.dbtune.seq.def.SeqIndex;
import edu.ucsc.dbtune.seq.def.SeqStepConf;
import edu.ucsc.dbtune.seq.utils.RTimerN;
import edu.ucsc.dbtune.util.Rt;

public class DATPaper {
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

    static class DATExp {
        WorkloadLoader loader;
        GnuPlot plot;
        GnuPlot plotWin;
        double plotX;
        String plotLabel;
        double m;
        double percentageUpdate;
        double spaceBudge;
        double l;
        double alpha;
        double beta;
        double avgCreateCost;
        double windowSize;
        File debugFile;
        boolean rerunExperiment;
    }

    public static boolean useRatio = false;
    public static boolean addTransitionCostToObjective = false;
    public static boolean eachWindowContainsOneQuery = false;
    public static boolean useDB2Optimizer = false;
    public static boolean verifyByDB2Optimizer = false;
    public static boolean noAlphaBeta = false;

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
    PrintStream ps;

    public void p(String format, Object... args) {
        Rt.showDate = false;
        String s = String.format(format, args);
        ps.println(s);
        Rt.p(s);
    }

    public DATPaper(DATExp p) throws Exception {
        ps = new PrintStream(new FileOutputStream(p.debugFile, true));
        cost = p.loader.loadCost();
        p(p.plot.name + " " + p.plot.xName + "=" + p.plotX);
        p("minCost: %,.0f", cost.costWithAllIndex);
        p("maxCost: %,.0f", cost.costWithoutIndex);

        if (eachWindowContainsOneQuery)
            p.m = cost.queries.size();
        cost.addTransitionCostToObjective = DATPaper.addTransitionCostToObjective;
        cost.eachWindowContainsOneQuery = DATPaper.eachWindowContainsOneQuery;
        // Rt.p(cost.queries.get(0)
        // .cost(cost.indices.toArray(new SeqInumIndex[0])));
        long totalCost = 0;
        for (int i = 0; i < cost.indices.size(); i++) {
            SeqInumIndex index = cost.indices.get(i);
            totalCost += index.createCost;
        }
        for (int i = 0; i < cost.queries.size(); i++) {
            SeqInumQuery query = cost.queries.get(i);
            if (!"select".equalsIgnoreCase(query.sql.getSQLCategory().name())) {
                query.weight = query.tableSize * p.percentageUpdate;
            }
        }
        int m = (int) p.m;
        int l = (int) p.l;
        windowConstraints = new double[m];

        if (p.alpha < 0 || p.alpha > 1)
            throw new Error();
        this.alpha = p.alpha;
        this.beta = p.beta;
        if (noAlphaBeta) {
            this.alpha = 1;
            this.beta = 1;
        }
        for (int i = 0; i < windowConstraints.length; i++)
            windowConstraints[i] = p.windowSize;
        cost.storageConstraint = p.spaceBudge;
        maxIndexCreatedPerWindow = l;
        String windowSizeS = String.format("%,.0f", p.windowSize);
        if (windowSizeS.length() > 20)
            windowSizeS = Double.toString(p.windowSize);
        p("windowSize=%s m=%d l=%d alpha=%f beta=%f space=%,.0f", windowSizeS, m, l, alpha, beta, p.spaceBudge);

        {
            SeqCost seqCost = eachWindowContainsOneQuery ? SeqCost.fromInum(cost) : SeqCost.multiWindows(cost, m);
            if (useDB2Optimizer) {
                seqCost.useDB2Optimizer = useDB2Optimizer;
                seqCost.db = p.loader.getDb();
                seqCost.optimizer = p.loader.getDB2Optimizer();
            }
            seqCost.addTransitionCostToObjective = cost.addTransitionCostToObjective;
            seqCost.stepBoost = new double[m + 1];
            Arrays.fill(seqCost.stepBoost, alpha);
            seqCost.stepBoost[m - 1] = beta; // last
            // window
            seqCost.storageConstraint = p.spaceBudge;
            seqCost.maxTransitionCost = p.windowSize;
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
                p("GREEDY-SEQ window " + i + ": cost=%,.0f createCost=%,.0f usedIndexes=%d", greedyWindowCosts[i], tc,
                        indices.length);
                if (i < m - 1)
                    this.greedySeq += alpha * greedyWindowCosts[i];
                else
                    this.greedySeq += beta * greedyWindowCosts[i];
            }
            if (!useDB2Optimizer && verifyByDB2Optimizer) {
                p("verifying window cost");
                for (int i = 0; i < m; i++) {
                    SeqStepConf conf = greedySeq.bestPath[i + 1];
                    DatabaseSystem db = p.loader.getDb();
                    DB2Optimizer optimizer = p.loader.getDB2Optimizer();
                    greedyWindowCosts[i] = seqCost.verifyCost(i, db, optimizer, conf)
                            + (seqCost.addTransitionCostToObjective ? conf.transitionCost : 0);
                    p("GREEDY-SEQ window " + i + ": cost=%,.0f usedIndexes=%d", greedyWindowCosts[i],
                            conf.configuration.indices.length);
                }
            }
            p("GREEDY-SEQ time: %.2f s", timer.getSecondElapse());
            p("Obj value: %,.0f", this.greedySeq);
            p("whatIfCount: %d", seqCost.whatIfCount);
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
                p("DAT Window %d: cost=%,.0f\tcreateCost=%,.0f\tusedIndexes=%d", i, datWindowCosts[i],
                        output.ws[i].createCost, output.ws[i].present);
            }
            if (useDB2Optimizer || verifyByDB2Optimizer) {
                p("verifying window cost");
                for (int i = 0; i < m; i++) {
                    DatabaseSystem db = p.loader.getDb();
                    DB2Optimizer optimizer = p.loader.getDB2Optimizer();
                    datWindowCosts[i] = dat.getWindowCost(i, db, optimizer);
                    p("DAT Window %d: cost=%,.0f\tcreateCost=%,.0f\tusedIndexes=%d", i, datWindowCosts[i],
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
            p("DAT time: %.2f s", timer.getSecondElapse());
            p("Obj value: %,.0f", this.dat);
            // dat = dsp.dat;
            // mkp = dsp.bip;
            // greedyRatio = dsp.greedy;
        }

        p.plot.startNewX(p.plotX, p.plotLabel);
        if (useRatio) {
            p.plot.usePercentage = true;
            p.plot.addY(dat / greedySeq * 100);
        } else {
            p.plot.addY(dat);
            p.plot.addY(greedySeq);
        }
        p.plotWin.startNewX(p.plotX, p.plotLabel);
        for (int i = 0; i < m; i++) {
            p.plotWin.addY(datWindowCosts[i]);
            p.plotWin.addY(greedyWindowCosts[i]);
        }
        p.loader.close();
        ps.close();
    }

    public static double getAlpha(double _1mada) {
        double alpha = 1 / (_1mada + 1);
        if (Math.abs((1 - alpha) / alpha - _1mada) > 1E-5)
            throw new Error();
        return alpha;
    }
}
