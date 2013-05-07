package edu.ucsc.dbtune.deployAware.test;

import java.io.File;

import edu.ucsc.dbtune.deployAware.test.DATPaper.DATExp;
import edu.ucsc.dbtune.deployAware.test.DATPaper.TestSet;
import edu.ucsc.dbtune.util.Rt;

public class DATPaperParams {
    public static interface Callback {
        void callback(TestSet set, DATExp p, double value);
    }

    public static class Set {
        double def;
        String name;
        double[] values;
        String[] names;
        Callback callback;

        public Set(double def, String name, double[] values, String[] names, Callback callback) {
            this.def = def;
            this.name = name;
            this.values = values;
            this.callback = callback;

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

    public Set m = new Set(3, "m", new double[] {2, 4, 8, 12, 16 }, null, new Callback() {
        @Override
        public void callback(TestSet set, DATExp p, double value) {
            p.m = value;
        }
    });
    public Set winFactor = new Set(-1, "window", new double[] { 2, 4, 8, 16, 1E100 }, new String[] { "2x", "4x",
            "8x", "16x", "INF" }, new Callback() {
        @Override
        public void callback(TestSet set, DATExp p, double value) {
            if (value < 0)
                p.windowSize = -1;
            else
                p.windowSize = p.avgCreateCost * value;
        }
    });
    public Set spaceFactor = new Set(0.1, "space", new double[] {0.05, 0.1, 0.5, 1, 5}, new String[] {
           "0.05x", "0.1x", "0.5x", "1x", "INF"}, new Callback() {
        @Override
        public void callback(TestSet set, DATExp p, double value) {
            p.spaceBudge = set.size * value;
        }
    });
    public Set l = new Set(-1, "l", new double[] { 1, 2, 4, 8, 100000 }, new String[] { "1", "2", "4", "8", "INF" },
            new Callback() {
                @Override
                public void callback(TestSet set, DATExp p, double value) {
                    p.l = value;
                }
            });
    public Set percentageUpdate = new Set(1E-3, "update", new double[] {0, 1E-3, 1E-2, 1E-1 }, new String[] {
            "0", "10^{-3}", "10^{-2}", "10^{-1}" }, new Callback() {
        @Override
        public void callback(TestSet set, DATExp p, double value) {
            p.percentageUpdate = value;
        }
    });
    public Set workloadRatio = new Set(1, "input size", new double[] { 1.0 / 8, 1.0 / 4, 1.0 / 2, 1 }, new String[] {
            "1/8", "1/4", "1/2", "1" }, new Callback() {
        @Override
        public void callback(TestSet set, DATExp p, double value) {
            p.workloadRatio = value;
            int newSize = (int) (p.cost.queries.size() * p.workloadRatio);
            Rt.p("reduce workload size: %d -> %d", p.cost.queries.size(), newSize);
            p.cost.queries.setSize(newSize);
        }
    });
    public Set indexRatio = new Set(1, "configuration size", new double[] { 1.0 / 8, 1.0 / 4, 1.0 / 2, 1 },
            new String[] { "1/8", "1/4", "1/2", "1" }, new Callback() {
                @Override
                public void callback(TestSet set, DATExp p, double value) {
                    p.indexRatio = value;
                }
            });
    public Set bipEpGap = new Set(0.1, "bip EpGap", new double[] { 0.05, 0.1, 0.15 },
            new String[] { "5%", "10%", "15%" }, new Callback() {
                @Override
                public void callback(TestSet set, DATExp p, double value) {
                    p.bipEpGap = value;
                }
            });
    public Set _1mada = new Set(0.5, "a", new double[] {0, 0.00125, 0.25, 0.5, 16, 1024}, null, new Callback() {
        @Override
        public void callback(TestSet set, DATExp p, double value) {
            p.alpha = DATPaper.getAlpha(value);
            p.beta = 1 - p.alpha;
        }
    });

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
