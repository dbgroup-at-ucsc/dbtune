package edu.ucsc.dbtune.deployAware.test;

import java.io.File;

public class DATPaperParams {
    String latex = DATSettings.latex;
    String pdflatex = DATSettings.pdflatex;
    File outputDir = DATSettings.outputDir;
    File figsDir = new File(outputDir, "figs");
    File skylineLatexFile = new File(outputDir, "skyline.tex");
    public int m_def = 3;
    public double spaceFactor_def = 0.5;
    public int l_def = 6;
    public double _1mada_def = 2;
    public int[] m_set = { 2, 3, 4, 5 };
    public double[] spaceFactor_set = { 0.25, 0.5, 1, 2, 4, 1000000 };
    public String[] spaceFactor_names = { "0.25x", "0.5x", "1x", "2x", "4x",
            "INF" };
    public int[] l_set = { 4, 6, 8 };
    public int[] _1mada_set = { 1, 2, 4, 16 };
    double[] tau_set = { 0.5, 0.6, 0.8, 0.9, 0.95, 1, 1.05, 1.1, 1.2, 1.4 };
    String scale = "0.7";
    boolean copyEps = false;
    String generateIndexMethod = "recommend";
    boolean exp5 = true; // rerun experiment
    long windowSize = 0;

    public DATPaperParams() {
        spaceFactor_def = 2;
        // spaceFactor_def=10;
        l_def = 4;
        l_def = 5;
        // l_def = 1000;
        _1mada_def = 1;
        exp5 = false;
    }
}
