package edu.ucsc.dbtune.deployAware.test;

import java.io.File;

import edu.ucsc.dbtune.deployAware.test.DATPaper.DATExp;

public class DATPaperParams {
    public static class Set {
        double def;
        double[] values;
        String[] names;

        public Set(double def, double[] values, String[] names) {
            this.def = def;
            this.values = values;
            if (names == null) {
                names = new String[values.length];
                for (int i = 0; i < values.length; i++) {
                    names[i] = "" + values[i];
                }
            }
            this.names = names;
        }
    }

    String latex = DATSettings.latex;
    String pdflatex = DATSettings.pdflatex;
    File outputDir = DATSettings.outputDir;
    File figsDir = new File(outputDir, "figs");
    File skylineLatexFile = new File(outputDir, "skyline.tex");

    public Set m = new Set(4, new double[] { 2, 4, 6 }, null);
    public Set winFactor = new Set(1E100, new double[] { 8, 16, 32, 64, 1E100 }, new String[] { "8x", "16x", "32x",
            "64x", "INF" });
    public Set spaceFactor = new Set(0.5, new double[] { 0.25, 0.5, 1, 1000000 }, new String[] { "0.25x", "0.5x", "1x",
            "INF" });
    public Set l = new Set(100000, new double[] { 4, 6, 8, 10, 100000 }, new String[] { "4", "6", "8", "10", "INF" });
    public Set percentageUpdate = new Set(1E-3, new double[] { 1E-5, 1E-4, 1E-3, 1E-2 }, new String[] { "10^{-5}",
            "10^{-4}", "10^{-3}", "10^{-2}" });

    public Set _1mada = new Set(1, new double[] { 1, 2, 4, 16 }, null);

    double[] tau_set = { 0.5, 0.6, 0.8, 0.9, 0.95, 1, 1.05, 1.1, 1.2, 1.4 };
    String scale = "0.7";
    boolean copyEps = false;
    String generateIndexMethod = "recommend";
    boolean exp5 = true; // rerun experiment
    long windowSize = 0;

    //    long windowSizeForce = 0;

    public DATPaperParams() {
        //        spaceFactor_def = 2;
        // spaceFactor_def=10;
        //        l_def = 4;
        //        l_def = 5;
        // l_def = 1000;
        //        _1mada_def = 1;
        exp5 = false;
    }

    DATExp def() {
        DATExp p = new DATExp();
        p.m = m.def;
        p.l = l.def;
        p.alpha = DATPaper.getAlpha(_1mada.def);
        p.beta = 1 - p.alpha;
        return p;
    }
}
