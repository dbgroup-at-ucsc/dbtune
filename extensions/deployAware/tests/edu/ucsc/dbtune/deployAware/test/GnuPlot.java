package edu.ucsc.dbtune.deployAware.test;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintStream;
import java.util.Vector;

public class GnuPlot {
    File dataFile;
    File pltFile;
    Vector<Vector<Double>> vs = new Vector<Vector<Double>>();
    Vector<Double> current = new Vector<Double>();

    public GnuPlot(File dir, String name) {
        dataFile = new File(dir, name + ".data");
        pltFile = new File(dir, name + ".plt");
    }

    public void add(double x, double y) {
        current.add(x);
        current.add(y);
    }

    public void addLine() {
        vs.add(current);
        current.removeAllElements();
    }

    public void finish() throws IOException {
        if (current.size() > 0)
            throw new Error();
        PrintStream ps = new PrintStream(dataFile);
        for (int i = 0; i < vs.size(); i++) {
            Vector<Double> current = vs.get(i);
            for (int j = 0; j < current.size(); j++) {
                if (j > 0)
                    ps.print("\t");
                ps.print(current.get(j));
            }
            if (i < vs.size() - 1)
                ps.println();
        }
        ps.close();
    }
}
