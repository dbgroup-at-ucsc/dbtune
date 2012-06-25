package edu.ucsc.dbtune.deployAware.test;

import java.io.File;
import java.io.PrintStream;
import java.io.StringReader;

import edu.ucsc.dbtune.bip.util.LogListener;
import edu.ucsc.dbtune.deployAware.DAT;
import edu.ucsc.dbtune.deployAware.DATOutput;
import edu.ucsc.dbtune.seq.bip.SeqInumCost;
import edu.ucsc.dbtune.seq.bip.def.SeqInumIndex;
import edu.ucsc.dbtune.seq.utils.Rt;
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
            double alpha) throws Exception {
        cost = DATTest2.loadCost();
        long totalCost = 0;
        for (int i = 0; i < cost.indices.size(); i++) {
            SeqInumIndex index = cost.indices.get(i);
            totalCost += index.createCost;
        }
        int windowSize = 5 * l * (int) (totalCost / cost.indices.size());
        windowConstraints = new double[m];
        if (alpha < 0 || alpha > 1)
            throw new Error();
        this.alpha = alpha;
        this.beta = 1 - alpha;
        for (int i = 0; i < windowConstraints.length; i++)
            windowConstraints[i] = windowSize;
        cost.storageConstraint = spaceBudge;
        maxIndexCreatedPerWindow = l;
        dat = dat();
        greedyRatio = baseline();
        mkp = mkp();
        plot.setPlotNames(new String[] { "DAT", "baseline", "MKP" });
        plot.add(plotX, dat);
        plot.add(plotX, greedyRatio);
        plot.add(plotX, mkp);
        plot.addLine();
        if (DATTest2.db != null)
            DATTest2.db.getConnection().close();
    }

    DAT getDat() throws Exception {
        DAT dat = new DAT(cost, windowConstraints, alpha, beta,maxIndexCreatedPerWindow);
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
        Rt.p(output.totalCost);
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
        exp = false;
        File outputDir = new File("/home/wangrui/dbtune/paper");
        File latexFile = new File(outputDir, "experiment.tex");

        PrintStream ps = new PrintStream(latexFile);
        ps.println("\\documentclass[10pt]{article}\n" + "\n"
                + "\\usepackage{graphicx}   % need for figures\n" + "\n"
                + "\\begin{document}\n" + "" + "");
        long gigbytes = 1024L * 1024L * 1024L;
        TestSet[] sets = {
                new TestSet("12 TPC-H queries", "tpch10g", "tpch-inum",
                        10 * gigbytes),
                new TestSet("12 TPC-H queries  \\& update stream RF1 and RF2",
                        "tpch10g", "tpch-benchmark-mix", 10 * gigbytes),
                new TestSet("100 OTAB [5] queries", "test",
                        "online-benchmark-100", 10 * gigbytes),
                new TestSet("100 OTAB [5] queries and 10 updates", "test",
                        "online-benchmark-update-100", 10 * gigbytes), };
        int m_def = 3;
        double spaceFactor_def = 0.5;
        int l_def = 6;
        double _1mada_def = 2;
        int[] m_set = { 2, 3, 4, 5 };
        double[] spaceFactor_set = { 0.25, 0.5, 1, 1000000 };
        String[] spaceFactor_names = { "0.25x", "0.5x", "1.0x", "INF" };
        int[] l_set = { 4, 6, 8, 50 };
        int[] _1mada_set = { 1, 2, 4, 16 };
        for (TestSet set : sets) {
            Rt.p(set.dbName + " " + set.workloadName);
            DATTest2.dbName = set.dbName;
            DATTest2.workloadName = set.workloadName;
            long spaceBudge = (long) (set.size * spaceFactor_def);
            GnuPlot.uniform=true;
            GnuPlot plot = new GnuPlot(outputDir, set.dbName + "X"
                    + set.workloadName + "Xm", "m", "cost");
            plot.setXtics(m_set);
            plot.setPlotNames(new String[] { "DAT", "baseline", "MKP" });
            ps.println("\\begin{figure}\n" + "\\centering\n"
                    + "\\includegraphics[scale=1]{" + plot.name + ".eps}\n"
                    + "\\caption{" + set.name + " m}\n" + "\\label{"
                    + plot.name + "}\n" + "\\end{figure}\n" + "");
            if (exp) {
                for (int m : m_set) {
                    new DATPaper(plot, m, m, spaceBudge, l_def,
                            getAlpha(_1mada_def));
                }
            }
            plot.finish();
            plot = new GnuPlot(outputDir, set.dbName + "X" + set.workloadName
                    + "Xspace", "space", "cost");
            plot.setPlotNames(new String[] { "DAT", "baseline", "MKP" });
            ps.println("\\begin{figure}\n" + "\\centering\n"
                    + "\\includegraphics[scale=1]{" + plot.name + ".eps}\n"
                    + "\\caption{" + set.name + " space}\n" + "\\label{"
                    + plot.name + "}\n" + "\\end{figure}\n" + "");
            plot.setXtics(spaceFactor_names, spaceFactor_set);
            if (exp) {
                for (double spaceFactor : spaceFactor_set) {
                    long space = (long) (set.size * spaceFactor);
                    new DATPaper(plot, spaceFactor, m_def, space, l_def,
                            getAlpha(_1mada_def));
                }
            }
            plot.finish();
            plot = new GnuPlot(outputDir, set.dbName + "X" + set.workloadName
                    + "Xl", "l", "cost");
            plot.setXtics(l_set);
            plot.setPlotNames(new String[] { "DAT", "baseline", "MKP" });
            ps.println("\\begin{figure}\n" + "\\centering\n"
                    + "\\includegraphics[scale=1]{" + plot.name + ".eps}\n"
                    + "\\caption{" + set.name + " l}\n" + "\\label{"
                    + plot.name + "}\n" + "\\end{figure}\n" + "");
            if (exp) {
                for (int l : l_set) {
                    new DATPaper(plot, l, m_def, spaceBudge, l,
                            getAlpha(_1mada_def));
                }
            }
            plot.finish();
            plot = new GnuPlot(outputDir, set.dbName + "X" + set.workloadName
                    + "X1mada", "(1-a)/a", "cost");
            plot.setXtics(_1mada_set);
            plot.setPlotNames(new String[] { "DAT", "baseline", "MKP" });
            ps.println("\\begin{figure}\n" + "\\centering\n"
                    + "\\includegraphics[scale=1]{" + plot.name + ".eps}\n"
                    + "\\caption{" + set.name + " alpha}\n" + "\\label{"
                    + plot.name + "}\n" + "\\end{figure}\n" + "");
            if (exp) {
                for (double _1mada : _1mada_set) {
                    double alpha = getAlpha(_1mada);
                    new DATPaper(plot, _1mada, m_def, spaceBudge, l_def, alpha);
                }
            }
            plot.finish();
        }
        ps.println("\\end{document}\n");
        ps.close();
        Rt.runAndShowCommand(
                "/data/texlive/2011/bin/i386-linux/xelatex -interaction=nonstopmode "
                        + latexFile.getName(), outputDir);
    }
}
