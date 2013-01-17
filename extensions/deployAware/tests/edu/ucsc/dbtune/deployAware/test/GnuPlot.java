package edu.ucsc.dbtune.deployAware.test;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.util.Vector;

import edu.ucsc.dbtune.util.Rt;

public class GnuPlot {
    public static boolean uniform = false;
    File dir;
    public String name;
    String xName;
    String yName;
    File dataFile;
    File orgDataFile;
    File pltFile;
    public String outputName;
    public Vector<Vector<Double>> vs = new Vector<Vector<Double>>();
    Vector<Double> current = new Vector<Double>();
    String[] plotNames;
    Vector<String> xtics = new Vector<String>();

    public GnuPlot(File dir, String name, String xName, String yName) {
        this.dir = dir;
        this.xName = xName;
        this.yName = yName;
        setName(name);
    }

    public void setName(String name) {
        this.name = name;
        dataFile = new File(dir, name + ".data");
        orgDataFile = new File(dir, name + "_org.data");
        pltFile = new File(dir, name + ".plt");
    }

    private void finishX() {
        if (current.size() > 0) {
            Vector<Double> v = new Vector<Double>();
            v.addAll(current);
            vs.add(v);
            current.removeAllElements();
        }
    }

    public void startNewX(double x) {
        startNewX(x, null);
    }

    public void startNewX(double x, String label) {
        finishX();
        current.add(x);
        if (label != null)
            xtics.add(label);
    }

    public void addY(double y) {
        current.add(y);
    }

    public void setPlotNames(String[] plotNames) {
        this.plotNames = plotNames;
    }

    public boolean firstColumnIsLabel = false;
    public boolean usePercentage = false;
    public boolean scale = true;
    public boolean logscale = false;
    public boolean xGrid = true;
    public boolean yGrid = true;
    public String extra;
    public String[] dataFiles;
    public String y2label;

    public enum Style {
        lines,
        points,
        linespoints,
        dots,
        impulses, //
        labels,
        steps,
        fsteps,
        histeps,
        errorbars, //
        errorlines,
        financebars,
        vectors,
        xerrorbar, //
        xerrorlines,
        xyerrorbars,
        xyerrorlines, //
        yerrorbars,
        yerrorlines, //
        // fill style
        boxes,
        boxerrorbars,
        boxxyerrorbars,
        boxplot, //
        candlesticks,
        filledcurves,
        histograms,
        image, //
        rgbimage,
        rgbalpha,
        circles,
        ellipses,
        pm3d,
    };

    public enum FillStyle {
        empty,
        solid,
        pattern
    };

    public static Style defaultStyle = Style.linespoints;
    public Style style = defaultStyle;

    private boolean isLineStyle() {
        return style.ordinal() < Style.boxes.ordinal();
    }

    private String getStyle(int i) {
        if (isLineStyle())
            return String.format("with " + style.name() + " lw 4 pt %d ps 2", 0, i);
        else
            return String.format("with " + style.name() + " fill solid %.1f", 0.2 + i * 0.2);
    }

    public String keyPosition = "right top";
    public String[] titles;

    public void finish() throws IOException {
        int power = 0;
        int plots = 0;
        double maxX = 0;
        double maxY = 0;
        double factor = 1;
        if (dataFiles == null) {
            finishX();
            if (current.size() > 0)
                throw new Error();
            if (vs.size() > 0) {
                if (!orgDataFile.getParentFile().exists())
                    orgDataFile.getParentFile().mkdirs();
                PrintStream ps = new PrintStream(orgDataFile);
                for (int i = 0; i < vs.size(); i++) {
                    Vector<Double> current = vs.get(i);
                    for (int j = 0; j < current.size(); j++) {
                        if (j > 0)
                            ps.print("\t");
                        String s = "" + current.get(j);
                        if (j == 0) {
                            if (xtics.size() > 0)
                                s += "," + xtics.get(i);
                        }
                        ps.print(s);
                    }
                    if (i < vs.size() - 1)
                        ps.println();
                }
                ps.close();
            }
            double minY = Double.MAX_VALUE;
            String[] lines = Rt.readFileAsLines(orgDataFile);
            int dsI = 0;
            for (String line : lines) {
                String[] ss = line.split("\t");
                String[] ss2 = ss[0].split(",", 2);
                double x = Double.parseDouble(ss2[0]);
                if (x > maxX)
                    maxX = x;
                for (int i = 1; i < ss.length; i++) {
                    double y = Double.parseDouble(ss[i]);
                    if (y > maxY)
                        maxY = y;
                    if (y < minY)
                        minY = y;
                    // if (x < 0 || y < 0)
                    // Rt.error("Can't handle negative value");
                }
            }
            power = (int) Math.floor(Math.log10(minY));
            factor = Math.pow(10, power);
            if (factor < 0.000001)
                factor = 1;
            if (!scale || usePercentage) {
                power = 0;
                factor = 1;
            }
            PrintStream ps = new PrintStream(dataFile);
            if (plotNames != null) {
                ps.print("x");
                for (int i = 0; i < plotNames.length; i++) {
                    ps.print("\t");
                    ps.print(plotNames[i]);
                }
                ps.println();
            }
            for (int i = 0; i < lines.length; i++) {
                String[] ss = lines[i].split("\t");
                plots = ss.length - 1;
                String[] ss2 = ss[0].split(",", 2);
                double x = uniform ? i : Double.parseDouble(ss2[0]);
                String s = Double.toHexString(x);
                if (Math.abs(x - (int) x) < 1E-10)
                    s = String.format("%.0f", x);
                if (ss2.length > 1)
                    s = ss2[1];
                ps.print(s);
                ps.print("\t");
                for (int j = 1; j < ss.length; j++) {
                    double y = Double.parseDouble(ss[j]);
                    ps.print("\t");
                    ps.format("%f", y / factor);
                }
                if (i < lines.length - 1)
                    ps.println();
            }
            ps.close();
        }
        PrintStream ps = new PrintStream(pltFile);
        ps.println("reset");
        ps.println("set terminal postscript eps enhanced monochrome 26");
        ps.println("set output \"" + (outputName != null ? outputName : name) + ".eps\"");
        ps.println("#set term x11 0");
        ps.println("set boxwidth 0.9 absolute");
        ps.println("set key inside " + keyPosition + " vertical noreverse noenhanced autotitles nobox");
        ps.println("set style histogram clustered gap 1 title offset character 0, 0, 0");
        ps.println("set datafile missing '-'");
        ps.println("set style data histograms");
        // ps.println("set xtics ()");
        ps.println("set xlabel offset 0,0.5 \"" + xName + "\"");
        ps.print("set ylabel offset 2,0 \"" + yName);
        if (power > 0)
            ps.print("(10^{" + power + "})");
        if (usePercentage)
            ps.print(" %");
        ps.println("\"");
        if (y2label != null)
            ps.println("set y2label  \"" + y2label + "\"");
        ps.println("set nomxtics");
        ps.println("set mytics -1");
        ps.println("set grid noxtics noytics");
        ps.println("set autoscale y");
        ps.println("set autoscale y2");
        // ps.println("#set xrange [ 0 : 4.6]");
        if (logscale) {
            ps.println("set logscale y");
            // ps.println("set yrange [ 1 : " + Math.ceil(maxY * 1.2 / factor)
            // + "]");
        } else {
            ps.println("set yrange [ 0 : " + Math.ceil(maxY * 1.2 / factor) + "]");
        }
        if (xGrid || yGrid) {
            ps.print("set grid");
            if (xGrid)
                ps.print(" xtics");
            if (yGrid)
                ps.print(" ytics");
            ps.println();
        }
        if (extra != null)
            ps.println(extra);
        if (plotNames != null)
            ps.println("set key autotitle columnhead");
        ps.println("");
        ps.print("plot ");
        if (dataFiles != null) {
            for (int i = 0; i < dataFiles.length; i++) {
                if (i > 0)
                    ps.print(", \\\n\t");
                ps.print("\"" + dataFiles[i] + "\" using 1:2 with linespoints lw 4 pt 0 ps 2");
            }
        } else {
            for (int i = 0; i < plots; i++) {
                if (i > 0)
                    ps.print(", \\\n\t");
                ps.print("\"" + name + ".data\" ");
                // if (i == 0) {
                if (style == Style.boxplot) {
                    ps.format("using (%d):%d %s", i + 1, i + 2, getStyle(i));
                } else if (xtics.size() > 0 || firstColumnIsLabel || !isLineStyle())
                    ps.format("using %d:xticlabels(1) %s", i + 2, getStyle(i));
                else
                    ps.format("using 1:%d %s", i + 2, getStyle(i));
                if (titles != null)
                    ps.format(" title \"%s\"", titles[i]);
                if (y2label != null)
                    ps.format(" axes x1y%d ", i + 1);
                // } else {
                // ps.format("'' using %d %s", i + 2, getStyle(i));
                // }
            }
        }
        ps.close();
        Rt.runAndShowCommand("gnuplot " + name + ".plt", dir);
    }
}