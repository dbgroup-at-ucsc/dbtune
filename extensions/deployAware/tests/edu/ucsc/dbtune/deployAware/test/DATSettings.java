package edu.ucsc.dbtune.deployAware.test;

import java.io.File;

import edu.ucsc.dbtune.seq.bip.WorkloadLoaderSettings;

public class DATSettings {
    final static String latex = "/data/b/soft/texlive/2011/bin/i386-linux/xelatex";
    final static String pdflatex = "/data/b/soft/texlive/2011/bin/i386-linux/pdflatex";
    final static File outputDir = new File(WorkloadLoaderSettings.dataRoot
            + "/paper");
}
