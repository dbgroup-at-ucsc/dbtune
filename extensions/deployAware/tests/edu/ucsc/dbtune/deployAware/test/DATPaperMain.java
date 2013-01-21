package edu.ucsc.dbtune.deployAware.test;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;

import edu.ucsc.dbtune.deployAware.test.DATPaper.DATExp;
import edu.ucsc.dbtune.deployAware.test.DATPaper.TestSet;
import edu.ucsc.dbtune.deployAware.test.DATPaperParams.Callback;
import edu.ucsc.dbtune.seq.SeqCost.QueryMap;
import edu.ucsc.dbtune.seq.bip.SeqInumCost;
import edu.ucsc.dbtune.seq.bip.WorkloadLoader;
import edu.ucsc.dbtune.util.Rt;

public class DATPaperMain {
    boolean windowOnly = false;
    boolean outputWinCost = false;
    DATPaperParams params = new DATPaperParams();
    boolean rerunAllExp = false;
    SimpleDateFormat df = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss");
    String rerunExperimentBeforeTime = "01/18/2013 16:40:01";
    long rerunTime = df.parse(rerunExperimentBeforeTime).getTime();
    long gigbytes = 1024L * 1024L * 1024L;
    long tpchWindowSize = 20 * 3600 * 3000;
    long tpcdsWindowSize = 10 * 3600 * 3000;
    File figsDir;
    TestSet tpch = new TestSet("TPCH", "tpch10g", "deployAware", "tpchPaper.sql", 10 * gigbytes, "tpch", tpchWindowSize);
    TestSet tpcds = new TestSet("TPCDS", "test", "deployAware", "tpcdsPaper.sql", 10 * gigbytes, "tpcds",
            tpcdsWindowSize);
    TestSet[] sets = { tpch, tpcds, };

    public DATPaperMain() throws Exception {
        Rt.p("Current time: " + df.format(new Date()));
        //        windowOnly = true;
        //         rerunAllExp=true;
        //        params.spaceFactor.def = 0.1;
        deployAwareTuning();
        workloadSequence();
        //         figsDir = new File(params.figsDir, "dat");
        //         figsDir.mkdirs();
        //         params.m.def=1;
        //        run(params.spaceFactor, "space", "Varying space budget", new Callback() {
        //            @Override
        //            public void callback(TestSet set, DATExp p, double value) {
        //                p.spaceBudge = set.size * value;
        //            }
        //        });
    }

    Callback defCallback;

    DATExp def(TestSet set, String name, String desc) throws Exception {
        GnuPlot.defaultStyle = GnuPlot.Style.histograms;
        GnuPlot.uniform = true;
        DATExp p = params.def();
        p.alpha = 1;
        p.beta = 1;
        WorkloadLoader loader = new WorkloadLoader(set.dbName, set.workloadName, set.fileName,
                params.generateIndexMethod);
        SeqInumCost cost = loader.loadCost();
        p.loader = loader;
        p.cost = cost;
        p.percentageUpdate = params.percentageUpdate.def;
        p.plot = new GnuPlot(figsDir, set.shortName + "X" + name, name, "cost");
        p.plot.setPlotNames(DATPaper.curNames);
        p.plotWin = new GnuPlot(figsDir, set.shortName + "X" + name + "W", name, "cost");
        p.plotWin.setPlotNames(DATPaper.curNames);
        set.plotNames.add(figsDir.getName() + "/" + p.plot.name);
        set.figureNames.add(desc);
        p.spaceBudge = (long) (set.size * params.spaceFactor.def);
        p.debugFile = new File(figsDir, p.plot.name + "_debug.txt");
        double sum = 0;
        for (int i = 0; i < cost.indexCount(); i++)
            sum += cost.indices.get(i).createCost;
        p.avgCreateCost = sum / cost.indexCount();
        if (params.winFactor.def < 0)
            p.windowSize = -1;
        else
            p.windowSize = params.winFactor.def * p.avgCreateCost;
        p.rerunExperiment = rerunAllExp || !p.plot.orgDataFile.exists()
                || p.plot.orgDataFile.lastModified() < rerunTime;
        if (defCallback != null)
            defCallback.callback(null, p, 0);
        return p;
    }

    public void run(TestSet set, DATPaperParams.Set inputs, String name, String desc, boolean useRunningTime,
            Callback callback) throws Exception {
        DATExp p = def(set, name, desc);
        p.plot.xName = inputs.name;
        if (useRunningTime) {
            p.plot.yName = "time";
            p.useRunningTime = useRunningTime;
        }
        if (p.rerunExperiment) {
            p.debugFile.delete();
            for (int k = 0; k < inputs.values.length; k++) {
                double value = inputs.values[k];
                p.plotX = value;
                p.plotLabel = inputs.names[k];
                if (callback != null)
                    callback.callback(set, p, value);
                inputs.callback.callback(set, p, value);
                new DATPaper(p);
            }
        }
        p.plot.finish();
        if (outputWinCost)
            p.plotWin.finish();
    }

    public void run(DATPaperParams.Set inputs, String name, String desc) throws Exception {
        for (TestSet set : sets)
            run(set, inputs, name, desc, false, null);
    }

    public void run2(DATPaperParams.Set inputs1, DATPaperParams.Set inputs2, String name, String desc) throws Exception {
        for (TestSet set : sets) {
            DATExp p = def(set, name, desc);
            Rt.p(p.plot.name);
            p.plot.pm3d = true;
            p.plot.set3dNames(inputs1.names, inputs2.names);
            p.plot.xName = inputs1.name;
            p.plot.yName = inputs2.name;
            if (p.rerunExperiment) {
                p.debugFile.delete();
                for (int i = 0; i < inputs1.values.length; i++) {
                    for (int j = 0; j < inputs2.values.length; j++) {
                        double value1 = inputs1.values[i];
                        double value2 = inputs2.values[j];
                        p.plotX = value1;
                        p.plotLabel = inputs1.names[i];
                        p.plotZ = value2;
                        p.plotLabelZ = inputs2.names[j];
                        inputs1.callback.callback(set, p, value1);
                        inputs2.callback.callback(set, p, value2);
                        new DATPaper(p);
                    }
                }
            }
            p.plot.finish();
            if (outputWinCost)
                p.plotWin.finish();
        }
    }

    public void windowOnly(String name, String desc, Callback callback) throws Exception {
        for (TestSet set : sets) {
            DATExp p = def(set, name, desc);
            p.plot.xName = "window";
            if (callback != null)
                callback.callback(set, p, 0);
            if (callback == null || p.rerunExperiment) {
                p.debugFile.delete();
                DATPaper run = new DATPaper(p);
                p.plot.vs.clear();
                p.plot.current.clear();
                p.plot.xtics.clear();
                for (int i = 0; i < run.datWindowCosts.length; i++) {
                    p.plot.startNewX(i);
                    p.plot.addY(run.datWindowCosts[i]);
                    // if (plotNames.length == 3)
                    // plot.addY(run.mkpWindowCosts[i]);
                    p.plot.addY(run.greedyWindowCosts[i]);
                }
            }
            p.plot.finish();
        }
    }

    public void workloadSequence() throws Exception {
        outputWinCost = false;
        DATPaper.addTransitionCostToObjective = true;
        defCallback = new Callback() {
            @Override
            public void callback(TestSet set, DATExp p, double value) {
                QueryMap[] map = new QueryMap[p.cost.queries.size()];
                for (int i = 0; i < map.length; i++)
                    map[i] = new QueryMap(i, i);
                p.queryMapping = map;
            }
        };
        DATPaper.noAlphaBeta = true;

        figsDir = new File(params.figsDir, "seq");
        figsDir.mkdirs();

        for (TestSet set : sets) {
            set.plotNames.clear();
            set.figureNames.clear();
        }
        if (windowOnly) {
            windowOnly("window", "cost of each window", null);
        } else {
            // useDB2Optimizer = true;
            // verifyByDB2Optimizer=true;
            run(params.spaceFactor, "space", "Varying space budget");
            run(params.winFactor, "window", "Varying window size");
            run(params.l, "l", "Varying numberof indexes in a window");
            run(params.percentageUpdate, "update", "Varying ratio of udpates");
            run2(params.spaceFactor, params.m, "spaceNm", "Varying space budget and number of windows");
            run2(params.spaceFactor, params.winFactor, "spaceNwin", "Varying space budget and window size");
            run(tpcds, params.workloadRatio, "inputSize", "Running time w.r.t. input size", true, null);
            run(tpcds, params.indexRatio, "indexSize", "Running time w.r.t. index size", true, null);
            run(tpcds, params.bipEpGap, "bipTime", "Running time w.r.t. bip EpGap", true, null);
            run(tpcds, params.bipEpGap, "bipQuality", "Result w.r.t. bip EpGap", false, null);
        }
        DATPaperOthers.generatePdf(new File(params.outputDir, "seq.tex"), params, sets);
    }

    public void deployAwareTuning() throws Exception {
        outputWinCost = false;
        DATPaper.addTransitionCostToObjective = false;
        defCallback = new Callback() {
            @Override
            public void callback(TestSet set, DATExp p, double value) {
                QueryMap[] map = new QueryMap[p.cost.queries.size() * (int) p.m];
                int pos = 0;
                for (int i = 0; i < p.m; i++) {
                    for (int j = 0; j < p.cost.queries.size(); j++) {
                        map[pos++] = new QueryMap(j, i);
                    }
                }
                p.queryMapping = map;
            }
        };
        DATPaper.noAlphaBeta = false;

        figsDir = new File(params.figsDir, "dat");
        figsDir.mkdirs();

        for (TestSet set : sets) {
            set.plotNames.clear();
            set.figureNames.clear();
        }

        if (windowOnly) {
            windowOnly("window", "cost of each window", null);
            return;
        }

        run(params.m, "m", "Varying number of windows");
        run(params.spaceFactor, "space", "Varying space budget");
        run(params.winFactor, "window", "Varying window size");
        run(params.l, "l", "Varying numberof indexes in a window");
        windowOnly("firstWindow", "Boost first window", new Callback() {
            @Override
            public void callback(TestSet set, DATExp p, double value) {
                double[] weights = new double[(int) params.m.def];
                Arrays.fill(weights, 1);
                weights[0] = 10;
                p.windowWeights = weights;
            }
        });
        windowOnly("mustDesc", "Cost must decrease", new Callback() {
            @Override
            public void callback(TestSet set, DATExp p, double value) {
                p.costMustDecrease = true;
            }
        });

        DATPaperOthers.generatePdf(new File(params.outputDir, "dat.tex"), params, sets);
    }

    public static void main(String[] args) throws Exception {
        new DATPaperMain();
    }
}
