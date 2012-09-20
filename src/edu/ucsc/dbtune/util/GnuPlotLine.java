package edu.ucsc.dbtune.util;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintStream;
import java.util.Vector;

import edu.ucsc.dbtune.util.Rt;

public class GnuPlotLine {
    public static boolean uniform = false;
    File dir;
    String name;
    String xName;
    String yName;
    File dataFile;
    File orgDataFile;
    File pltFile;
    Vector<Vector<Double>> vs = new Vector<Vector<Double>>();
    Vector<Double> current = new Vector<Double>();
    String[] plotNames;
    String[] xtics;
    double[] xticsValues;

    public GnuPlotLine(File dir, String name, String xName, String yName) {
        this.dir = dir;
        this.name = name;
        this.xName = xName;
        this.yName = yName;
        dataFile = new File(dir, name + ".data");
        orgDataFile = new File(dir, name + "_org.data");
        pltFile = new File(dir, name + ".plt");
    }

    public void setXtics(int[] xticsValues) {
        this.xtics = new String[xticsValues.length];
        this.xticsValues = new double[xticsValues.length];
        for (int i = 0; i < this.xtics.length; i++) {
            this.xtics[i] = String.format("%d", xticsValues[i]);
            this.xticsValues[i] = xticsValues[i];
        }
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
        if (vs.size() > 0) {
            PrintStream ps = new PrintStream(orgDataFile);
            for (int i = 0; i < vs.size(); i++) {
                Vector<Double> current = vs.get(i);
                for (int j = 0; j < current.size(); j++) {
                    if (j > 0)
                        ps.print("\t");
                    if (j % 2 == 0)
                        ps.print(current.get(j));
                    else
                        ps.print(current.get(j));
                }
                if (i < vs.size() - 1)
                    ps.println();
            }
            ps.close();
        }
        double maxX = 0;
        double maxY = 0;
        double minY = Double.MAX_VALUE;
        String[] lines = Rt.readFileAsLines(orgDataFile);
        for (String line : lines) {
            String[] ss = line.split("\t");
            for (int i = 0; i < ss.length; i += 2) {
                double x = Double.parseDouble(ss[i]);
                double y = Double.parseDouble(ss[i + 1]);
                if (x > maxX)
                    maxX = x;
                if (y > maxY)
                    maxY = y;
                if (y < minY)
                    minY = y;
                if (x < 0 || y < 0)
                    Rt.error("Can't handle negative value");
            }
        }
        maxY *= 1.1;
        int power = (int) Math.floor(Math.log10(minY));
        // If the value is less than 100, no need to turn into
        // power
        if (maxY <= 110)
            power = 0;
        double factor = Math.pow(10, power);
        if (factor < 0.000001)
            factor = 1;
        PrintStream ps = new PrintStream(dataFile);
        for (int i = 0; i < lines.length; i++) {
            String[] ss = lines[i].split("\t");
            for (int j = 0; j < ss.length; j += 2) {
                double x = Double.parseDouble(ss[j]);
                double y = Double.parseDouble(ss[j + 1]);
                // for (int i = 0; i < vs.size(); i++) {
                // Vector<Double> current = vs.get(i);
                // for (int j = 0; j < current.size(); j++) {
                if (j > 0)
                    ps.print("\t");
                ps.format("%.5f", uniform ? i : x);
                ps.print("\t");
                ps.format("%.5f", y / factor);
            }
            if (i < lines.length - 1)
                ps.println();
        }
        ps.close();
        ps = new PrintStream(pltFile);
        ps.println("reset");
        ps.println("set terminal postscript eps enhanced monochrome 26");
        ps.println("set output \"" + name + ".eps\"");
        // ps.println("#set xrange [ 0 : 4.6]");
        ps.println("set yrange [ " + (Math.ceil(minY / factor) - 1) + " : " + Math.ceil(maxY / factor) + "]");
        // ps.println("#set logscale y");
        ps.println("set xlabel offset 0,0.5 \"" + xName + "\"");
        ps.print("set ylabel offset 2,0 \"" + yName);
        if (power > 0)
            ps.print("(x 10^{" + power + "})");
        ps.println("\"");
        ps.println("set nomxtics");
        ps.println("set mytics -1");
        ps.println("set grid noxtics noytics");
        if (xtics != null) {
            ps.print("set xtics (");
            for (int i = 0; i < xtics.length; i++) {
                if (i > 0)
                    ps.print(",");
                ps.print("\"" + xtics[i] + "\" "
                        + (uniform ? i : xticsValues[i]));
            }
            ps.println(")");
        } else {
            ps.print("set xtics (");
            for (int i = 0; i < xticsValues.length; i++) {
                if (i > 0)
                    ps.print(",");
                String s = Double.toString(xticsValues[i]);
                if (Math.abs(xticsValues[i] - (int) xticsValues[i]) < 1E-10)
                    s = String.format("%.0f", xticsValues[i]);
                ps.print("\"" + s + "\" " + (uniform ? i : xticsValues[i]));
            }
            ps.println(")");
        }
        // ps.println("#set ytics 10");
        ps.println("set size 1, 0.8");
        ps.println("set boxwidth 0.3");
        // ps.println("set key at 3.9, 8.45");
        ps.println("set key spacing 1.2");
        ps.println("");
        for (int i = 0; i < plotNames.length; i++) {
            if (i == 0)
                ps.print("plot ");
            else
                ps.print(" ");
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