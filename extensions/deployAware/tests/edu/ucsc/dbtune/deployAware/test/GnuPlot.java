package edu.ucsc.dbtune.deployAware.test;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.util.Hashtable;
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
    Vector<String> ztics = new Vector<String>();

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

    public void startNewX(double x, String label, double z, String zlabel) {
        pm3d = true;
        finishX();
        current.add(x);
        current.add(z);
        if (label != null)
            xtics.add(label);
        if (zlabel != null)
            ztics.add(zlabel);
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
    public boolean pm3d = false;
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
                        if (pm3d && j == 1) {
                            if (ztics.size() > 0)
                                s += "," + ztics.get(i);
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
                for (int i = pm3d ? 2 : 1; i < ss.length; i++) {
                    double y = Double.parseDouble(ss[i]);
                    if (y > maxY)
                        maxY = y;
                    if (y < minY)
                        minY = y;
                    // if (x < 0 || y < 0)
                    // Rt.error("Can't handle negative value");
                }
            }
            power = (int) Math.floor(Math.log10(maxY));
            factor = Math.pow(10, power);
            if (factor < 0.000001)
                factor = 1;
            if (!scale || usePercentage) {
                power = 0;
                factor = 1;
            }
            if (pm3d) {
                finish3d();
                return;
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
        if (pm3d) {
            finish3d();
            return;
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
        if (power != 0)
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

    String[] xname3d;
    String[] yname3d;

    void set3dNames(String[] x, String[] y) {
        this.xname3d = x;
        this.yname3d = y;
    }

    void finish3d() throws IOException {
        double[][][] ds = new double[xname3d.length][yname3d.length][];
        Hashtable<String, Integer> xh = new Hashtable<String, Integer>();
        Hashtable<String, Integer> yh = new Hashtable<String, Integer>();
        for (int i = 0; i < xname3d.length; i++)
            xh.put(xname3d[i], i);
        for (int i = 0; i < yname3d.length; i++) {
            //            Rt.p(yname3d[i]);
            yh.put(yname3d[i], i);
        }
        String[] lines = Rt.readFileAsLines(orgDataFile);
        double max = 0;
        for (int i = 0; i < lines.length; i++) {
            String[] ss = lines[i].split("\t");
            String[] x = ss[0].split(",", 2);
            String[] y = ss[1].split(",", 2);
            double[] d = new double[ss.length - 2];
            for (int j = 2; j < ss.length; j++) {
                double d2 = Double.parseDouble(ss[j]);
                if (d2 > max)
                    max = d2;
                d[j - 2] = d2;
            }
            d[0] = d[0] / d[1];
            Integer t1 = xh.get(x[1]);
            if (t1 == null)
                throw new Error(x[1]);
            Integer t2 = yh.get(y[1]);
            if (t2 == null)
                throw new Error(y[1]);
            ds[t1][t2] = d;
        }
        //        double power = (int) Math.floor(Math.log10(max)) - 1;
        //        double factor = Math.pow(10, power);
        //        for (double[][] ds2 : ds) {
        //            for (double[] ds3 : ds2) {
        //                for (int i = 0; i < ds3.length; i++) {
        //                    ds3[i] /= factor;
        //                }
        //            }
        //        }

        uniform = true;
        int pt = 0;
        PrintStream ps = new PrintStream(dataFile.getAbsolutePath());
        for (int xi = 0; xi < xh.size(); xi++) {
            double start = 0;
            double end = 1;
            for (int yi = 0; yi < yh.size(); yi++) {
                ps.format("%f %s %f %s %f\n", xi + start, xname3d[xi], yi + start, yname3d[yi], ds[xi][yi][pt]);
                ps.format("%f %s %f %s %f\n", xi + start, xname3d[xi], yi + end, yname3d[yi], ds[xi][yi][pt]);
            }
            ps.println();
            for (int yi = 0; yi < yh.size(); yi++) {
                ps.format("%f %s %f %s %f\n", xi + end, xname3d[xi], yi + start, yname3d[yi], ds[xi][yi][pt]);
                ps.format("%f %s %f %s %f\n", xi + end, xname3d[xi], yi + end, yname3d[yi], ds[xi][yi][pt]);
            }
            ps.println();
        }
        ps.close();

        ps = new PrintStream(pltFile);

        ps.println("reset");
        ps.println("set terminal postscript eps enhanced monochrome 26");
        ps.println("set output \"" + (outputName != null ? outputName : name) + ".eps\"");
        ps.println("set pm3d");
        ps.println("set palette gray");
        ps.println("set grid x y z");
        ps.println("set xlabel \"" + xName + "\"");
        ps.println("set ylabel \"" + yName + "\"");
        ps.print("set xtics (");
        for (int i = 0; i < xname3d.length; i++) {
            if (i > 0)
                ps.print(",");
            ps.print("\"" + xname3d[i] + "\" " + (i + 0.5));
        }
        ps.println(")");
        ps.print("set ytics (");
        for (int i = 0; i < yname3d.length; i++) {
            if (i > 0)
                ps.print(",");
            ps.print("\"" + yname3d[i] + "\" " + (i + 0.5));
        }
        ps.println(")");

        ps.println("splot '" + dataFile.getName() + "' using 1:3:5 with pm3d title \"" + plotNames[0] + "/"
                + plotNames[1] + "\"");
        ps.close();
        Rt.runAndShowCommand("gnuplot " + name + ".plt", dir);
    }
}