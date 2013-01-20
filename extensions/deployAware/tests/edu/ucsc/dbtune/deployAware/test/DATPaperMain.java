package edu.ucsc.dbtune.deployAware.test;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.Date;

import edu.ucsc.dbtune.deployAware.test.DATPaper.DATExp;
import edu.ucsc.dbtune.deployAware.test.DATPaper.TestSet;
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
    TestSet[] sets = { //
    new TestSet("TPCH", "tpch10g", "deployAware", "tpchPaper.sql", 10 * gigbytes, "tpch",//
            tpchWindowSize),
    //            new TestSet("TPCDS", "test", "deployAware", "tpcdsPaper.sql", 10 * gigbytes, "tpcds", tpcdsWindowSize), 
    };

    public DATPaperMain() throws Exception {
        Rt.p("Current time: " + df.format(new Date()));
        //        windowOnly = true;
        //         rerunAllExp=true;
        //        params.spaceFactor.def = 0.1;
        //        deployAwareTuning();
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
        return p;
    }

    public void run(DATPaperParams.Set inputs, String name, String desc) throws Exception {
        for (TestSet set : sets) {
            DATExp p = def(set, name, desc);
            if (p.rerunExperiment) {
                p.debugFile.delete();
                for (int k = 0; k < inputs.values.length; k++) {
                    double value = inputs.values[k];
                    p.plotX = value;
                    p.plotLabel = inputs.names[k];
                    inputs.callback.callback(set, p, value);
                    new DATPaper(p);
                }
            }
            p.plot.finish();
            if (outputWinCost)
                p.plotWin.finish();
        }
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

    public void windowOnly() throws Exception {
        for (TestSet set : sets) {
            DATExp p = def(set, "window", "cost of each window");
            if (true || p.rerunExperiment) {
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
        DATPaper.eachWindowContainsOneQuery = true;
        DATPaper.noAlphaBeta = true;

        figsDir = new File(params.figsDir, "seq");
        figsDir.mkdirs();

        for (TestSet set : sets) {
            set.plotNames.clear();
            set.figureNames.clear();
        }
        if (windowOnly) {
            windowOnly();
        } else {
            // useDB2Optimizer = true;
            // verifyByDB2Optimizer=true;
            run(params.spaceFactor, "space", "Varying space budget");
            run(params.winFactor, "window", "Varying window size");
            run(params.l, "l", "Varying numberof indexes in a window");
            run(params.percentageUpdate, "update", "Varying ratio of udpates");
            run2(params.spaceFactor, params.m, "spaceNm", "Varying space budget and number of windows");
            run2(params.spaceFactor, params.winFactor, "spaceNwin", "Varying space budget and window size");
        }
        DATPaperOthers.generatePdf(new File(params.outputDir, "seq.tex"), params, sets);
    }

    public void deployAwareTuning() throws Exception {
        outputWinCost = false;
        DATPaper.addTransitionCostToObjective = false;
        DATPaper.eachWindowContainsOneQuery = false;
        DATPaper.noAlphaBeta = false;

        figsDir = new File(params.figsDir, "dat");
        figsDir.mkdirs();

        for (TestSet set : sets) {
            set.plotNames.clear();
            set.figureNames.clear();
        }

        if (windowOnly) {
            windowOnly();
            return;
        }

        run(params.m, "m", "Varying number of windows");

        run(params.spaceFactor, "space", "Varying space budget");

        run(params.winFactor, "window", "Varying window size");

        run(params.l, "l", "Varying numberof indexes in a window");

        DATPaperOthers.generatePdf(new File(params.outputDir, "dat.tex"), params, sets);
    }

    public static void main(String[] args) throws Exception {
        new DATPaperMain();
    }
}
