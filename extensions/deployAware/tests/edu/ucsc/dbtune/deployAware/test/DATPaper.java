package edu.ucsc.dbtune.deployAware.test;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.io.StringReader;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.Vector;

import edu.ucsc.dbtune.bip.util.LogListener;
import edu.ucsc.dbtune.deployAware.DAT;
import edu.ucsc.dbtune.deployAware.DATOutput;
import edu.ucsc.dbtune.seq.bip.SeqInumCost;
import edu.ucsc.dbtune.seq.bip.def.SeqInumIndex;
import edu.ucsc.dbtune.seq.utils.RTimer;
import edu.ucsc.dbtune.seq.utils.RTimerN;
import edu.ucsc.dbtune.seq.utils.Rt;
import edu.ucsc.dbtune.seq.utils.Rx;
import edu.ucsc.dbtune.workload.Workload;

public class DATPaper {
    public static String[] plotNames = new String[] { "DAT",
    // "optimal",
            "DA" };
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
                loader.generateIndexMethod, alpha, beta, m, l, spaceBudge,
                windowSize, 0);
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
        plot.setPlotNames(plotNames);
        plot.add(plotX, dat);
        if (dsp.runMKP)
            plot.add(plotX, mkp);
        plot.add(plotX, greedyRatio);
        // plot.add(plotX, dsp.datWindowCosts[0]);
        // plot.add(plotX, dsp.greedyWindowCosts[0]);
        plot.addLine();
        loader.close();
    }

    public static class TestSet {
        String name;
        String shortName;
        String dbName;
        String workloadName;
        long size;
        Vector<String> plotNames = new Vector<String>();
        Vector<String> figureNames = new Vector<String>();

        public TestSet(String name, String dbName, String workloadName,
                long size, String shortName) {
            this.name = name;
            this.dbName = dbName;
            this.workloadName = workloadName;
            this.size = size;
            this.shortName = shortName;
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
        boolean exp5 = true; // rerun experiment
        boolean scalabilityTest = true;
        // exp = false;
        exp5 = false;
        scalabilityTest = false;
        boolean copyEps = false;
        File outputDir = new File("/home/wangrui/dbtune/paper");
        File figsDir = new File(outputDir, "figs");
        File latexFile2 = new File(outputDir, "skyline.tex");
        String generateIndexMethod = "recommend";
        // generateIndexMethod = "powerset 2";

        // ps.println("\\begin{multicols}{2}\n");
        long gigbytes = 1024L * 1024L * 1024L;
        TestSet[] sets = {
                // new TestSet("TPCDS", "test", "tpcds-inum",
                // gigbytes),
//                new TestSet("12 TPC-H queries", "tpch10g", "tpch-inum",
//                        10 * gigbytes, "TPC-H"),
//                new TestSet("12 TPC-H queries  \\& update stream RF1 and RF2",
//                        "tpch10g", "tpch-benchmark-mix", 10 * gigbytes,
//                        "TPC-H update"),
                 new TestSet("25 TPCDS queries",
                 "test", "tpcds-inum", 10 * gigbytes,"TPCDS25"),
                // new TestSet("170 OST queries", "test", "OST", 10 * gigbytes),
//                new TestSet("100 OTAB  queries", "test",
//                        "online-benchmark-100", 10 * gigbytes, "OTAB"),
//                new TestSet("100 OTAB queries and 10 updates", "test",
//                        "online-benchmark-update-100", 10 * gigbytes,
//                        "OTAB update"),
                        };
        if (false) {
            for (TestSet set : sets) {
                Rt.p(set.dbName + " " + set.workloadName);
                PrintStream ps = new PrintStream("/home/wangrui/dbtune/index/"
                        + set.dbName + "_" + set.workloadName + ".txt");
                WorkloadLoader loader = new WorkloadLoader(set.dbName,
                        set.workloadName, generateIndexMethod);
                SeqInumCost cost = loader.loadCost();
                Vector<SeqInumIndex> indices = new Vector<SeqInumIndex>();
                indices.addAll(cost.indices);
                Collections.sort(indices, new Comparator<SeqInumIndex>() {
                    @Override
                    public int compare(SeqInumIndex o1, SeqInumIndex o2) {
                        return -(int) (o1.indexBenefit - o2.indexBenefit);
                    }
                });
                ps.format("fts_cost: %,.2f\n",cost.costWithoutIndex);
                Rt.np("fts_cost: %,.2f\n",cost.costWithoutIndex);
                Rt.np("optimal_cost: %,.2f\n",cost.costWithAllIndex);
                for (int i = 0; i < indices.size(); i++) {
                    SeqInumIndex index = indices.get(i);
                    ps.format("%.2f\t%.2f\t%s\n", index.createCost / 1000000,
                            index.indexBenefit / 1000000, index
                                    .getColumnNames());
                    Rt.np(cost.indices.get(i));
                }
                ps.close();
            }
            System.exit(0);
        }
        if (false) {
            for (String method : new String[] { "recommend", "powerset 2" }) {
                for (TestSet set : sets) {
                    WorkloadLoader loader = new WorkloadLoader(set.dbName,
                            set.workloadName, method);
                    loader.getIndexes();
                }
            }
            System.exit(0);
        }
        int m_def = 3;
        double spaceFactor_def = 0.5;
        spaceFactor_def = 2;
        // spaceFactor_def=10;
        int l_def = 6;
        l_def = 4;
        // l_def=20;
        // l_def = 1000;
        double _1mada_def = 2;
        _1mada_def = 1;
        int[] m_set = { 2, 3, 4, 5, 10 };
        double[] spaceFactor_set = { 0.25, 0.5, 1, 2, 4, 1000000 };
        String[] spaceFactor_names = { "0.25x", "0.5x", "1x", "2x", "4x", "INF" };
        int[] l_set = { 4, 6, 8, 20, 40 };
        int[] _1mada_set = { 1, 2, 4, 16 };
        double[] tau_set = { 0.5, 0.6, 0.8, 0.9, 0.95, 1, 1.05, 1.1, 1.2, 1.4 };
        String scale = "0.7";

        int windowSize = 0;
        File debugDir = new File("/home/wangrui/dbtune/debug");
        for (TestSet set : sets) {
            // SeqInumCost cost = DATTest2.loadCost();
            // long totalCost = 0;
            // for (int i = 0; i < cost.indices.size(); i++) {
            // SeqInumIndex index = cost.indices.get(i);
            // totalCost += index.createCost;
            // }
            // int windowSize = 3600 * 3000;// 13 * (int) (totalCost /
            Rt.p(set.dbName + " " + set.workloadName);
            WorkloadLoader loader = new WorkloadLoader(set.dbName,
                    set.workloadName, generateIndexMethod);
            set.plotNames.clear();
            set.figureNames.clear();

            long spaceBudge = (long) (set.size * spaceFactor_def);
            GnuPlot2.uniform = true;
            GnuPlot2 plot = null;

            if (true) {
                plot = new GnuPlot2(figsDir, set.dbName + "X"
                        + set.workloadName + "Xm", "m", "cost");
                plot.setXtics(m_set);
                plot.setPlotNames(plotNames);
                set.plotNames.add(plot.name);
                set.figureNames.add("differnt window size");
                if (exp) {
                    for (int m : m_set) {
                        File debugFile = new File(debugDir, plot.name + "_" + m
                                + ".xml");
                        new DATPaper(loader, plot, m, m, spaceBudge, l_def,
                                getAlpha(_1mada_def), windowSize, debugFile);
                    }
                }
                plot.finish();
                plot = new GnuPlot2(figsDir, set.dbName + "X"
                        + set.workloadName + "Xspace", "space", "cost");
                plot.setPlotNames(plotNames);
                set.plotNames.add(plot.name);
                set.figureNames.add("different space budget");
                plot.setXtics(spaceFactor_names, spaceFactor_set);
                if (exp) {
                    int i = 0;
                    for (double spaceFactor : spaceFactor_set) {
                        long space = (long) (set.size * spaceFactor);
                        File debugFile = new File(debugDir, plot.name + "_"
                                + spaceFactor_names[i] + ".xml");
                        new DATPaper(loader, plot, spaceFactor, m_def, space,
                                l_def, getAlpha(_1mada_def), windowSize,
                                debugFile);
                        i++;
                    }
                }
                plot.finish();
                plot = new GnuPlot2(figsDir, set.dbName + "X"
                        + set.workloadName + "Xl", "l", "cost");
                plot.setXtics(l_set);
                plot.setPlotNames(plotNames);
                set.plotNames.add(plot.name);
                set.figureNames.add("maximum indexes per window");
                if (exp) {
                    for (int l : l_set) {
                        File debugFile = new File(debugDir, plot.name + "_" + l
                                + ".xml");
                        new DATPaper(loader, plot, l, m_def, spaceBudge, l,
                                getAlpha(_1mada_def), windowSize, debugFile);
                    }
                }
                plot.finish();
                plot = new GnuPlot2(figsDir, set.dbName + "X"
                        + set.workloadName + "X1mada", "(1-a)/a", "cost");
                plot.setXtics(_1mada_set);
                plot.setPlotNames(plotNames);
                set.plotNames.add(plot.name);
                set.figureNames.add("alpha");
                if (exp) {
                    for (int _1mada : _1mada_set) {
                        double alpha = getAlpha(_1mada);
                        File debugFile = new File(debugDir, plot.name + "_"
                                + _1mada + ".xml");
                        new DATPaper(loader, plot, _1mada, m_def, spaceBudge,
                                l_def, alpha, windowSize, debugFile);
                    }
                }
                plot.finish();
            }
            int win = m_def;
            plot = new GnuPlot2(figsDir, set.dbName + "X" + set.workloadName
                    + "Xwindow", "window", "cost");
            int[] set2 = new int[win];
            for (int i = 0; i < set2.length; i++)
                set2[i] = i;
            plot.setXtics(set2);
            plot.setPlotNames(plotNames);
            set.plotNames.add(plot.name);
            set.figureNames.add("cost of each window");
            if (exp) {
                File debugFile = new File(debugDir, plot.name + "_"
                        + "window.xml");
                DATPaper run = new DATPaper(loader, plot, win, win, spaceBudge,
                        l_def, getAlpha(_1mada_def), windowSize, debugFile);
                plot.vs.clear();
                for (int i = 0; i < win; i++) {
                    plot.add(i, run.dsp.datWindowCosts[i]);
                    if (plotNames.length == 3)
                        plot.add(i, run.dsp.mkpWindowCosts[i]);
                    plot.add(i, run.dsp.greedyWindowCosts[i]);
                    plot.addLine();
                }
            }
            plot.finish();
        }

        String latex = "/usr/local/texlive/2011/bin/i386-linux/xelatex";
        {
            File latexFile = new File(outputDir, generateIndexMethod.replace(
                    ' ', '_')
                    + ".tex");
            PrintStream ps = new PrintStream(latexFile);
            ps.println("\\documentclass{vldb}}\n" + "\n"
                    + "\\usepackage{graphicx}   % need for figures\n" + "\n"
                    + "\\usepackage{subfigure}\n" + "\n"
                    + "\\begin{document}\n" + "" + "");
            ps.println("\\textbf{default values:}\\\\");
            ps.println("windowSize=" + windowSize + "\\\\");
            ps.println("spaceFactor=" + spaceFactor_def + "\\\\");
            ps.println("l=" + l_def + "\\\\");
            ps.println("m=" + m_def + "\\\\");
            ps.println("(1-a)/a=" + _1mada_def + "\\\\");
            ps.println();
            for (int i = 0; i < sets[0].plotNames.size(); i++) {
                File outputDir2 = new File(
                        "/home/wangrui/dbtune/dbtune.papers/deployment_aware_tuning/figs");
                ps.println("\\begin{figure}\n" + "\\centering\n");
                for (TestSet set : sets) {
                    ps
                            .println("        \\begin{subfigure}["
                                    + set.shortName
                                    + "]\n"
                                    + "                \\centering\n"
                                    + "                \\includegraphics[scale=0.3]{figs/"
                                    + set.plotNames.get(i) + ".eps}\n"
                                    + "        \\end{subfigure}");
                    // ps.println("\\begin{figure}\n" + "\\centering\n"
                    // + "\\includegraphics[scale=" + scale + "]{figs/"
                    // + set.plotNames.get(i) + ".eps}\n" + "\\caption{"
                    // + set.figureNames.get(i) + "}\n"
                    // // + "\\label{" + plot.name + "}\n"
                    // + "\\end{figure}\n" + "");
                    if (copyEps)
                        Rt.copyFile(new File(figsDir, set.plotNames.get(i)
                                + ".eps"), new File(outputDir2, set.plotNames
                                .get(i)
                                + ".eps"));
                }
                ps.println("\\caption{" + sets[0].figureNames.get(i) + "}\n"
                        + "\\end{figure}");
            }
            ps.println("\\end{document}\n");
            ps.close();
            Rt.runAndShowCommand(latex + " -interaction=nonstopmode "
                    + latexFile.getName(), outputDir);
        }
        for (int i = 0; i < sets[0].plotNames.size(); i++) {
            File latexFile = new File(outputDir, "_exp_" + (i + 1) + ".tex");
            // for (TestSet set : sets) {
            // File latexFile = new File(outputDir, "_" + set.workloadName
            // + ".tex");
            // Rt.runAndShowCommand(latex + " -interaction=nonstopmode "
            // + latexFile.getName(), outputDir);
        }

        if (false) {
            PrintStream ps = new PrintStream(latexFile2);
            ps.println("\\documentclass{vldb}}\n" + "\n"
                    + "\\usepackage{graphicx}   % need for figures\n" + "\n"
                    + "\\usepackage{subfigure}\n" + "\n"
                    + "\\begin{document}\n" + "" + "");
            for (TestSet set : sets) {
                Rt.p(set.dbName + " " + set.workloadName);
                long spaceBudge = (long) (set.size * spaceFactor_def);
                GnuPlot2.uniform = true;
                GnuPlot2 plot = new GnuPlot2(outputDir, set.dbName + "X"
                        + set.workloadName + "Xskyline", "r", "cost");
                plot.setXtics(null, tau_set);
                plot.setPlotNames(new String[] { "DAT" });
                ps.println("\\begin{figure}\n" + "\\centering\n"
                        + "\\includegraphics[scale=" + scale + "]{" + plot.name
                        + ".eps}\n" + "\\caption{" + set.name + " skyline}\n"
                        // + "\\label{" + plot.name + "}\n"
                        + "\\end{figure}\n" + "");
                if (exp5) {
                    double alpha = 0.000001;
                    double beta = 1 - alpha;
                    DATSeparateProcess dsp = new DATSeparateProcess(set.dbName,
                            set.workloadName, generateIndexMethod, alpha, beta,
                            m_def, l_def, spaceBudge, windowSize, 0);
                    dsp.run();
                    double p0int = dsp.datIntermediate;
                    for (double tau : tau_set) {
                        dsp = new DATSeparateProcess(set.dbName,
                                set.workloadName, generateIndexMethod, alpha,
                                beta, m_def, l_def, spaceBudge, windowSize,
                                p0int * tau);
                        dsp.run();
                        plot.add(tau, dsp.dat);
                        plot.addLine();
                    }
                }
                plot.finish();
            }
            // ps.println("\\end{multicols}");
            ps.println("\\end{document}\n");
            ps.close();
            Rt.runAndShowCommand(latex + " -interaction=nonstopmode "
                    + latexFile2.getName(), outputDir);
        }
        // latex = "pdflatex";

        // detailed performance test
        if (false) {
            TestSet perfTest = sets[3];
            DATSeparateProcess dsp = new DATSeparateProcess(perfTest.dbName,
                    perfTest.workloadName, generateIndexMethod, 0.5, 0.5,
                    m_def, l_def, Double.MAX_VALUE, windowSize, 0);
            dsp.generatePerfReport = true;
            dsp.run();
        }

        // scalability test
        if (scalabilityTest) {
            // 0 3
            TestSet perfTest = sets[3];
            WorkloadLoader loader = new WorkloadLoader(perfTest.dbName,
                    perfTest.workloadName, generateIndexMethod);
            SeqInumCost cost = loader.loadCost();

            PrintStream ps2 = new PrintStream(
                    "/home/wangrui/dbtune/scalability.txt");
            int start = 1;
            int step = 1;
            int end = start + 100 * step;
            for (int i = start; i <= end; i += step) {
                DATSeparateProcess dsp = new DATSeparateProcess(
                        perfTest.dbName, perfTest.workloadName,
                        generateIndexMethod, 0.5, 0.5, m_def, l_def,
                        Double.MAX_VALUE, windowSize, 0);
                dsp.runGreedy = false;
                dsp.runMKP = false;
                dsp.dupWorkloadNTimes = i;
                // dsp.generatePerfReport = true;
                RTimerN timer = new RTimerN();
                dsp.run();
                ps2.format(cost.queries.size() * i + "\t%.3f\t%d\t%s\t%s\n",
                        timer.getSecondElapse(), dsp.memoryUsed,
                        perfTest.dbName + "_" + perfTest.workloadName,
                        new Date().toString());
                ps2.flush();
            }
            ps2.close();
        }
    }
}
