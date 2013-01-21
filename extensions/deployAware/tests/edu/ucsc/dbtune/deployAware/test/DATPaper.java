package edu.ucsc.dbtune.deployAware.test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.util.Arrays;
import java.util.Vector;

import edu.ucsc.dbtune.DatabaseSystem;
import edu.ucsc.dbtune.deployAware.DAT;
import edu.ucsc.dbtune.deployAware.DATOutput;
import edu.ucsc.dbtune.deployAware.DATParameter;
import edu.ucsc.dbtune.optimizer.DB2Optimizer;
import edu.ucsc.dbtune.seq.SeqCost;
import edu.ucsc.dbtune.seq.SeqGreedySeq;
import edu.ucsc.dbtune.seq.SeqCost.QueryMap;
import edu.ucsc.dbtune.seq.bip.SeqInumCost;
import edu.ucsc.dbtune.seq.bip.WorkloadLoader;
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

    public static class DATExp {
        WorkloadLoader loader;
        SeqInumCost cost;
        GnuPlot plot;
        GnuPlot plotWin;
        double plotX;
        String plotLabel;
        double plotZ;
        String plotLabelZ;
        double m;
        double percentageUpdate;
        double spaceBudge;
        double l;
        double alpha;
        double beta;
        double avgCreateCost;
        double windowSize;
        double workloadRatio = 1;
        double indexRatio = 1;
        double bipEpGap = 0.05;
        boolean useRunningTime = false;
        boolean costMustDecrease = false;
        public double[] windowWeights;
        File debugFile;
        boolean rerunExperiment;
        /**
         * Specify list of queries in each window
         */
        public int[][] queryMap;

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append(String.format("m=%f l=%f\n", m, l));
            sb.append(String.format("alpha=%f beta=%f \n", alpha, beta));
            sb.append(String.format("windowSize=%f\n", windowSize));
            sb.append(String.format("space=%,.0f\n", spaceBudge));
            sb.append(String.format("plotX=%f plotLabel=%s\n", plotX, plotLabel));
            sb.append(String.format("plotZ=%f plotLabelZ=%s\n", plotZ, plotLabelZ));
            sb.append(String.format("percentageUpdate=%f\n", percentageUpdate));
            sb.append(String.format("avgCreateCost=%f\n", avgCreateCost));
            sb.append(String.format("workloadRatio=%f\n", workloadRatio));
            sb.append(String.format("indexRatio=%f\n", indexRatio));
            sb.append(String.format("bipEpGap=%f\n", bipEpGap));
            sb.append(String.format("useRunningTime=" + useRunningTime + "\n"));
            sb.append(String.format("costMustDecrease=" + costMustDecrease + "\n"));
            if (windowWeights != null) {
                sb.append(String.format("windowWeights: "));
                for (int i = 0; i < windowWeights.length; i++) {
                    sb.append(windowWeights[i] + ",");
                    if (i % 16 == 15)
                        sb.append("\n");
                }
                sb.append("\n");
            }
            if (queryMap != null) {
                sb.append(String.format("queryMapping:\n"));
                for (int i = 0; i < queryMap.length; i++) {
                    sb.append("Window " + i + ": ");
                    for (int j = 0; j < queryMap[i].length; j++) {
                        sb.append(queryMap[i][j] + ",");
                        if (j % 16 == 15)
                            sb.append("\n");
                    }
                    sb.append("\n");
                }
            }
            return sb.toString();
        }
    }

    public static boolean useRatio = false;
    public static boolean addTransitionCostToObjective = false;
    //    public static boolean eachWindowContainsOneQuery = false;
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
        cost = p.cost;
        p(p.plot.name + " " + p.plot.xName + "=" + p.plotX);
        if (p.workloadRatio > 0.999) {
            p("minCost: %,.0f", cost.costWithAllIndex);
            p("maxCost: %,.0f", cost.costWithoutIndex);
        } else {
            int newSize = (int) (cost.queries.size() * p.workloadRatio);
            p("reduce workload size: %d -> %d", cost.queries.size(), newSize);
            cost.queries.setSize(newSize);
        }
        if (p.indexRatio < 0.999) {
            int newSize = (int) (cost.indices.size() * p.indexRatio);
            p("reduce index size: %d -> %d", cost.indices.size(), newSize);
            cost.reduceIndexSize(newSize);
        }
        if (p.costMustDecrease)
            p("cost must decrease");

        if (p.queryMap != null)
            p.m = p.queryMap.length;
        cost.addTransitionCostToObjective = DATPaper.addTransitionCostToObjective;
        cost.queryMap = p.queryMap;
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
        p(p.toString());
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

        double greedyRunningTime = 0;
        double datRunningTime = 0;
        {
            SeqCost seqCost = p.queryMap == null ? SeqCost.fromInum(cost) : SeqCost.multiWindows(cost,
                    p.queryMap);
            if (useDB2Optimizer) {
                seqCost.useDB2Optimizer = useDB2Optimizer;
                seqCost.db = p.loader.getDb();
                seqCost.optimizer = p.loader.getDB2Optimizer();
            }
            seqCost.addTransitionCostToObjective = cost.addTransitionCostToObjective;
            seqCost.stepBoost = new double[m + 1];
            Arrays.fill(seqCost.stepBoost, alpha);
            seqCost.stepBoost[m - 1] = beta; // last
            if (p.windowWeights != null) {
                for (int i = 0; i < p.windowWeights.length; i++)
                    seqCost.stepBoost[i] *= p.windowWeights[i];
            }
            // window
            seqCost.storageConstraint = p.spaceBudge;
            seqCost.maxTransitionCost = p.windowSize;
            seqCost.maxIndexesWindow = l;
            seqCost.costMustDecrease = p.costMustDecrease;
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
                long space = 0;
                for (SeqIndex index : indices)
                    space += index.inumIndex.storageCost;
                p("GREEDY-SEQ window " + i + ": cost=%,.0f createCost=%,.0f usedIndexes=%d space=%,d",
                        greedyWindowCosts[i], tc, indices.length, space);
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
            greedyRunningTime = timer.getSecondElapse();
            p("GREEDY-SEQ time: %.2f s", timer.getSecondElapse());
            p("Obj value: %,.0f", this.greedySeq);
            p("whatIfCount: %d", seqCost.whatIfCount);
        }

        {
            RTimerN timer = new RTimerN();
            DATParameter params = new DATParameter(cost, windowConstraints, alpha, beta, l);
            params.windowWeight = p.windowWeights;
            params.costMustDecrease = p.costMustDecrease;
            DAT dat = new DAT();
            DATOutput output = dat.runDAT(params, p.bipEpGap);
            this.dat = output.totalCost;
            datWindowCosts = new double[m];
            for (int i = 0; i < m; i++) {
                datWindowCosts[i] = output.ws[i].cost;
                long space = 0;
                for (int j = 0; j < output.ws[i].indexUsed.length; j++)
                    if (output.ws[i].indexUsed[j])
                        space += cost.indices.get(j).storageCost;
                p("DAT Window %d: cost=%,.0f createCost=%,.0f usedIndexes=%d create=%d drop=%d space=%,d", i,
                        datWindowCosts[i], output.ws[i].createCost, output.ws[i].present, output.ws[i].create,
                        output.ws[i].drop, space);
            }
            if (useDB2Optimizer || verifyByDB2Optimizer) {
                p("verifying window cost");
                for (int i = 0; i < m; i++) {
                    DatabaseSystem db = p.loader.getDb();
                    DB2Optimizer optimizer = p.loader.getDB2Optimizer();
                    datWindowCosts[i] = dat.getWindowCost(i, db, optimizer);
                    p("DAT Window %d: cost=%,.0f\tcreateCost=%,.0f\tusedIndexes=%d\tcreate=%d\tdrop=%d", i,
                            datWindowCosts[i], output.ws[i].createCost, output.ws[i].present, output.ws[i].create,
                            output.ws[i].drop);
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
            datRunningTime = timer.getSecondElapse();
            p("DAT time: %.2f s", timer.getSecondElapse());
            p("Obj value: %,.0f", this.dat);
            // dat = dsp.dat;
            // mkp = dsp.bip;
            // greedyRatio = dsp.greedy;
            dat.close();
        }

        if (p.plotLabelZ != null) {
            p.plot.startNewX(p.plotX, p.plotLabel, p.plotZ, p.plotLabelZ);
        } else {
            p.plot.startNewX(p.plotX, p.plotLabel);
        }
        if (p.useRunningTime) {
            dat = datRunningTime;
            greedySeq = greedyRunningTime;
        }
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
