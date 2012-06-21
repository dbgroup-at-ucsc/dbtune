package edu.ucsc.dbtune.deployAware.test;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintStream;
import java.util.Vector;

import edu.ucsc.dbtune.seq.utils.Rt;

public class GnuPlot {
    File dir;
    String name;
    String xName;
    String yName;
    File dataFile;
    File pltFile;
    Vector<Vector<Double>> vs = new Vector<Vector<Double>>();
    Vector<Double> current = new Vector<Double>();
    String[] plotNames;
    String[] xtics;
    double[] xticsValues;

    public GnuPlot(File dir, String name, String xName, String yName) {
        this.dir = dir;
        this.name = name;
        this.xName = xName;
        this.yName = yName;
        dataFile = new File(dir, name + ".data");
        pltFile = new File(dir, name + ".plt");
    }

    public void setXtics(String[] xtics, double[] xticsValues) {
        this.xtics = xtics;
        this.xticsValues = xticsValues;
    }

    public void add(double x, double y) {
        current.add(x);
        current.add(y);
    }

    public void addLine() {
        Vector<Double> v = new Vector<Double>();
        v.addAll(current);
        vs.add(v);
        current.removeAllElements();
    }

    public void setPlotNames(String[] plotNames) {
        this.plotNames = plotNames;
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
        ps = new PrintStream(pltFile);
        ps.println("reset");
        ps.println("set terminal postscript eps enhanced monochrome 26");
        ps.println("set output \"" + name + ".eps\"");
        // ps.println("#set xrange [ 0 : 4.6]");
        // ps.println("#set yrange [ 1.5 : 8.5]");
        // ps.println("#set logscale y");
        ps.println("set xlabel offset 0,0.5 \"" + xName + "\"");
        ps.println("set ylabel offset 2,0 \"" + yName + "\"");
        ps.println("set nomxtics");
        ps.println("set mytics -1");
        ps.println("set grid noxtics noytics");
        if (xtics != null) {
            ps.print("set xtics (");
            for (int i = 0; i < xtics.length; i++) {
                if (i > 0)
                    ps.print(",");
                ps.print("\"" + xtics[i] + "\" " + xticsValues[i]);
            }
            ps.println(")");
        }
        // ps.println("#set ytics 10");
        ps.println("set size 1, 0.8");
        ps.println("set boxwidth 0.3");
        ps.println("set key at 3.9, 8.45");
        ps.println("set key spacing 1.2");
        ps.println("");
        for (int i = 0; i < plotNames.length; i++) {
            if (i==0)
                ps.print("plot ");
            else
                ps.print("     ");
            ps.print("\"" + name + ".data\" using " + (i * 2 + 1) + ":"
                    + (i * 2 + 2) + " title \"" + plotNames[i]
                    + "\" with linespoints lw 4 pt " + (i * 2 + 2) + " ps 2");
            if (i < plotNames.length - 1)
                ps.print(", \\");
            ps.println();

        }
        ps.close();
        Rt.runAndShowCommand("gnuplot " + name + ".plt", dir);
    }
}
