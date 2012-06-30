package edu.ucsc.dbtune.deployAware.test;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.io.StringReader;

import edu.ucsc.dbtune.bip.util.LogListener;
import edu.ucsc.dbtune.deployAware.DAT;
import edu.ucsc.dbtune.deployAware.DATOutput;
import edu.ucsc.dbtune.seq.bip.SeqInumCost;
import edu.ucsc.dbtune.seq.bip.def.SeqInumIndex;
import edu.ucsc.dbtune.seq.utils.RTimer;
import edu.ucsc.dbtune.seq.utils.Rt;
import edu.ucsc.dbtune.seq.utils.Rx;
import edu.ucsc.dbtune.workload.Workload;

public class DATPaper {
    SeqInumCost cost;
    double[] windowConstraints;
    double alpha, beta;
    int maxIndexCreatedPerWindow = 0;
    double dat;
    double greedyRatio;
    double mkp;

    public DATPaper(GnuPlot plot, double plotX, int m, long spaceBudge, int l,
            double alpha, int windowSize) throws Exception {
        // m=3;
        // spaceBudge=20*1024L*1024L*1024L;
        // l=10;
        // alpha=0.5;
        cost = DATTest2.loadCost();
        long totalCost = 0;
        for (int i = 0; i < cost.indices.size(); i++) {
            SeqInumIndex index = cost.indices.get(i);
            totalCost += index.createCost;
        }
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
        DATSeparateProcess dsp = new DATSeparateProcess(DATTest2.dbName,
                DATTest2.workloadName, alpha, beta, m, l, spaceBudge,
                windowSize, 0);
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
        plot.setPlotNames(new String[] { "DAT", "baseline", "MKP" });
        plot.add(plotX, dat);
        plot.add(plotX, greedyRatio);
        plot.add(plotX, mkp);
        plot.addLine();
        if (DATTest2.db != null)
            DATTest2.db.getConnection().close();
    }

    DAT getDat() throws Exception {
        DAT dat = new DAT(cost, windowConstraints, alpha, beta,
                maxIndexCreatedPerWindow);
        LogListener logger = LogListener.getInstance();
        dat.setLogListenter(logger);
        dat.setWorkload(new Workload("", new StringReader("")));
        return dat;
    }

    double dat() throws Exception {
        // dat.setOptimizer(optimizer);
        DAT dat = getDat();
        dat.buildBIP();
        DATOutput output = (DATOutput) dat.getOutput();
        // System.out.print(alpha + ", " + beta + "\t");
        // for (int i = 0; i < windowConstraints.length; i++) {
        // System.out.format("%.0f\tc " + output.ws[i].create + ", d "
        // + output.ws[i].drop + "\t", output.ws[i].cost);
        // }
        Rt.p("dat=" + output.totalCost);
        return output.totalCost;
    }

    double baseline() throws Exception {
        DAT dat = getDat();
        DATOutput baseline = (DATOutput) dat.baseline2("greedyRatio");
        return baseline.totalCost;
    }

    double mkp() throws Exception {
        DAT dat = getDat();
        DATOutput baseline3 = (DATOutput) dat.baseline2("bip");
        Rt.p("mkp=" + baseline3.totalCost);
        return baseline3.totalCost;
    }

    public static class TestSet {
        String name;
        String dbName;
        String workloadName;
        long size;

        public TestSet(String name, String dbName, String workloadName,
                long size) {
            this.name = name;
            this.dbName = dbName;
            this.workloadName = workloadName;
            this.size = size;
        }
    }

    public static double getAlpha(double _1mada) {
        double alpha = 1 / (_1mada + 1);
        if (Math.abs((1 - alpha) / alpha - _1mada) > 1E-5)
            throw new Error();
        return alpha;
    }

    public static void main(String[] args) throws Exception {
        DATTest2.querySize = 0;
        DATTest2.indexSize = 0;
        boolean exp = true; // rerun experiment
        boolean exp5 = true; // rerun experiment
        exp = false;
        exp5 = false;
        File outputDir = new File("/home/wangrui/dbtune/paper");
        File latexFile2 = new File(outputDir, "skyline.tex");

        // ps.println("\\begin{multicols}{2}\n");
        long gigbytes = 1024L * 1024L * 1024L;
        TestSet[] sets = {
                new TestSet("12 TPC-H queries", "tpch10g", "tpch-inum",
                        10 * gigbytes),
                new TestSet("12 TPC-H queries  \\& update stream RF1 and RF2",
                        "tpch10g", "tpch-benchmark-mix", 10 * gigbytes),
                new TestSet("170 OST queries", "test", "OST", 10 * gigbytes),
                new TestSet("100 OTAB  queries", "test",
                        "online-benchmark-100", 10 * gigbytes),
                new TestSet("100 OTAB queries and 10 updates", "test",
                        "online-benchmark-update-100", 10 * gigbytes), 
                        };
        int windowSize = 3600 * 3000;// 13 * (int) (totalCost /
        int m_def = 3;
        double spaceFactor_def = 0.5;
        spaceFactor_def = 2;
        int l_def = 6;
        l_def = 10;// 2 def=4 8
        l_def = 1000;
        double _1mada_def = 2;
        _1mada_def = 1;
        int[] m_set = { 2, 3, 4, 5 };
        double[] spaceFactor_set = { 1, 2, 4, 1000000 };
        String[] spaceFactor_names = { "1x", "2x", "4x", "INF" };
        int[] l_set = { 4, 6, 8, 50 };
        int[] _1mada_set = { 1, 2, 4, 16 };
        double[] tau_set = { 0.5, 0.6, 0.8, 0.9, 0.95, 1, 1.05, 1.1, 1.2, 1.4 };
        String scale = "0.7";

        for (TestSet set : sets) {
            Rt.p(set.dbName + " " + set.workloadName);
            DATTest2.dbName = set.dbName;
            DATTest2.workloadName = set.workloadName;

            File latexFile = new File(outputDir, "_" + set.workloadName
                    + ".tex");
            PrintStream ps = new PrintStream(latexFile);
            ps.println("\\documentclass[10pt]{article}\n" + "\n"
                    + "\\usepackage{graphicx}   % need for figures\n" + "\n"
                    + "\\begin{document}\n" + "" + "");
            ps.println("\\textbf{default values:}\\\\");
            ps.println("windowSize=" + windowSize + "\\\\");
            ps.println("spaceFactor=" + spaceFactor_def + "\\\\");
            ps.println("l=" + l_def + "\\\\");
            ps.println("m=" + m_def + "\\\\");
            ps.println("(1-a)/a=" + _1mada_def + "\\\\");
            ps.println();

            long spaceBudge = (long) (set.size * spaceFactor_def);
            GnuPlot.uniform = true;
            GnuPlot plot = new GnuPlot(outputDir, set.dbName + "X"
                    + set.workloadName + "Xm", "m", "cost");
            plot.setXtics(m_set);
            plot.setPlotNames(new String[] { "DAT", "greedy", "MKP" });
            ps.println("\\begin{figure}\n" + "\\centering\n"
                    + "\\includegraphics[scale=" + scale + "]{" + plot.name
                    + ".eps}\n" + "\\caption{" + set.name + " m}\n"
                    // + "\\label{" + plot.name + "}\n"
                    + "\\end{figure}\n" + "");
            if (exp) {
                for (int m : m_set) {
                    new DATPaper(plot, m, m, spaceBudge, l_def,
                            getAlpha(_1mada_def), windowSize);
                }
            }
            plot.finish();
            plot = new GnuPlot(outputDir, set.dbName + "X" + set.workloadName
                    + "Xspace", "space", "cost");
            plot.setPlotNames(new String[] { "DAT", "greedy", "MKP" });
            ps.println("\\begin{figure}\n" + "\\centering\n"
                    + "\\includegraphics[scale=" + scale + "]{" + plot.name
                    + ".eps}\n" + "\\caption{" + set.name + " space}\n"
                    // + "\\label{" + plot.name + "}\n"
                    + "\\end{figure}\n" + "");
            plot.setXtics(spaceFactor_names, spaceFactor_set);
            if (exp) {
                for (double spaceFactor : spaceFactor_set) {
                    long space = (long) (set.size * spaceFactor);
                    new DATPaper(plot, spaceFactor, m_def, space, l_def,
                            getAlpha(_1mada_def), windowSize);
                }
            }
            plot.finish();
            plot = new GnuPlot(outputDir, set.dbName + "X" + set.workloadName
                    + "Xl", "l", "cost");
            plot.setXtics(l_set);
            plot.setPlotNames(new String[] { "DAT", "greedy", "MKP" });
            ps.println("\\begin{figure}\n" + "\\centering\n"
                    + "\\includegraphics[scale=" + scale + "]{" + plot.name
                    + ".eps}\n" + "\\caption{" + set.name + " l}\n"
                    // + "\\label{" + plot.name + "}\n"
                    + "\\end{figure}\n" + "");
            if (exp) {
                for (int l : l_set) {
                    new DATPaper(plot, l, m_def, spaceBudge, l,
                            getAlpha(_1mada_def), windowSize);
                }
            }
            plot.finish();
            plot = new GnuPlot(outputDir, set.dbName + "X" + set.workloadName
                    + "X1mada", "(1-a)/a", "cost");
            plot.setXtics(_1mada_set);
            plot.setPlotNames(new String[] { "DAT", "greedy", "MKP" });
            ps.println("\\begin{figure}\n" + "\\centering\n"
                    + "\\includegraphics[scale=" + scale + "]{" + plot.name
                    + ".eps}\n" + "\\caption{" + set.name + " alpha}\n"
                    // + "\\label{" + plot.name + "}\n"
                    + "\\end{figure}\n" + "");
            if (exp) {
                for (double _1mada : _1mada_set) {
                    double alpha = getAlpha(_1mada);
                    new DATPaper(plot, _1mada, m_def, spaceBudge, l_def, alpha,
                            windowSize);
                }
            }
            plot.finish();
            ps.println("\\end{document}\n");
            ps.close();
        }
        PrintStream ps = new PrintStream(latexFile2);
        ps.println("\\documentclass[10pt]{article}\n" + "\n"
                + "\\usepackage{graphicx}   % need for figures\n" + "\n"
                + "\\begin{document}\n" + "" + "");
        for (TestSet set : sets) {
            Rt.p(set.dbName + " " + set.workloadName);
            DATTest2.dbName = set.dbName;
            DATTest2.workloadName = set.workloadName;
            long spaceBudge = (long) (set.size * spaceFactor_def);
            GnuPlot.uniform = true;
            GnuPlot plot = new GnuPlot(outputDir, set.dbName + "X"
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
                DATSeparateProcess dsp = new DATSeparateProcess(
                        DATTest2.dbName, DATTest2.workloadName, alpha, beta,
                        m_def, l_def, spaceBudge, windowSize, 0);
                double p0int = dsp.datIntermediate;
                for (double tau : tau_set) {
                    dsp = new DATSeparateProcess(DATTest2.dbName,
                            DATTest2.workloadName, alpha, beta, m_def, l_def,
                            spaceBudge, windowSize, p0int * tau);
                    plot.add(tau, dsp.dat);
                    plot.addLine();
                }
            }
            plot.finish();
        }
        // ps.println("\\end{multicols}");
        ps.println("\\end{document}\n");
        ps.close();
        for (TestSet set : sets) {
            File latexFile = new File(outputDir, "_" + set.workloadName
                    + ".tex");
            Rt.runAndShowCommand(
                    "/data/texlive/2011/bin/i386-linux/xelatex -interaction=nonstopmode "
                            + latexFile.getName(), outputDir);
        }
        Rt.runAndShowCommand(
                "/data/texlive/2011/bin/i386-linux/xelatex -interaction=nonstopmode "
                        + latexFile2.getName(), outputDir);
    }
}
