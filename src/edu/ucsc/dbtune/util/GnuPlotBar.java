package edu.ucsc.dbtune.util;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.util.Vector;

/**
 * Automatic draw figures in GnuPlot
 * 
 * 
 * @author Rui Wang
 * @author Quoc Trung Tran
 *
 */
public class GnuPlotBar 
{   
    public static boolean uniform = false;
    public Vector<Vector<Double>> vs = new Vector<Vector<Double>>();
    
    File dir;
    String name;
    String xName;
    String yName;
    File dataFile;
    File orgDataFile;
    File pltFile;
    
    Vector<Double> current = new Vector<Double>();
    String[] plotNames;
    String[] xtics;
    double[] xticsValues;

    public GnuPlotBar(File dir, String name, String xName, String yName) 
    {
        this.dir = dir;
        this.name = name;
        this.xName = xName;
        this.yName = yName;
        dataFile = new File(dir, name + ".data");
        orgDataFile = new File(dir, name + "_org.data");
        pltFile = new File(dir, name + ".plt");
    }

    /**
     * Set the ticks in x-axis
     * 
     * @param xticsValues
     */
    public void setXtics(int[] xticsValues) 
    {
        this.xtics = new String[xticsValues.length];
        this.xticsValues = new double[xticsValues.length];
        
        for (int i = 0; i < xtics.length; i++) {
            this.xtics[i] = String.format("%d", xticsValues[i]);
            this.xticsValues[i] = xticsValues[i];
        }
    }

    /**
     * Set x-tics with the name and values are given
     * 
     * @param xtics
     * @param xticsValues
     */
    public void setXtics(String[] xtics, double[] xticsValues) 
    {
        this.xtics = xtics;
        this.xticsValues = xticsValues;
    }

    /**
     * Add a point into the plot
     * 
     * @param x
     *      x-axis value
     * @param y
     *      y-axis value
     */
    public void add(double x, double y) 
    {
        current.add(x);
        current.add(y);
    }

    /**
     * todo
     */
    public void addLine() 
    {
        Vector<Double> v = new Vector<Double>();
        v.addAll(current);
        vs.add(v);
        current.removeAllElements();
    }

    /**
     * todo
     * @param plotNames
     */
    public void setPlotNames(String[] plotNames) 
    {
        this.plotNames = plotNames;
    }

    /**
     * todo
     * @throws IOException
     */
    public void finish() throws IOException 
    {
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
        int power = (int) Math.floor(Math.log10(minY));
        double factor = Math.pow(10, power);
        if (factor < 0.000001)
            factor = 1;
        PrintStream ps = new PrintStream(dataFile);
        ps.print("x");
        for (int i = 0; i < plotNames.length; i++) {
            ps.print("\t");
            ps.print(plotNames[i]);
        }
        ps.println();
        for (int i = 0; i < lines.length; i++) {
            String[] ss = lines[i].split("\t");
            String s = Double.toString(xticsValues[i]);
            if (Math.abs(xticsValues[i] - (int) xticsValues[i]) < 1E-10)
                s = String.format("%.0f", xticsValues[i]);
            if (xtics != null)
                s = xtics[i];
            ps.print(s);
            ps.print("\t");
            for (int j = 0; j < ss.length; j += 2) {
                double y = Double.parseDouble(ss[j + 1]);
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

        ps.println("set boxwidth 0.9 absolute");
        ps.println("set key inside right top vertical Right noreverse noenhanced autotitles nobox");
        ps.println("set style histogram clustered gap 1 title offset character 0, 0, 0");
        ps.println("set datafile missing '-'");
        ps.println("set style data histograms");
        ps.println("#set xtics border in scale 0,0 nomirror rotate by -45 offset character 0, 0, 0");
        ps.println("#set xtics norangelimit font \",8\"");
        ps.println("set xtics ()");
        ps.println("#set title \"abc\" ");
        ps.println("set yrange [ 0 : 19.0]");
        ps.println("set xlabel offset 0,0.5 \"" + xName + "\"");
        ps.print("set ylabel offset 2,0 \"" + yName);
        if (power > 0)
            ps.print("(10^{" + power + "})");
        ps.println("\"");
        ps.println("set nomxtics");
        ps.println("set mytics -1");
        ps.println("set grid noxtics noytics");
        // ps.println("#set xrange [ 0 : 4.6]");
        ps.println("set yrange [ 0 : " + Math.ceil(maxY * 1.2 / factor) + "]");
        // ps.println("#set logscale y");
        
        ps.println("");
        ps.print("plot \"" + name + ".data\" ");
        for (int i = 0; i < plotNames.length; i++) {
            if (i == 0) {
                ps.print("using 2:xtic(1) fs solid 0.2 ti col");
            } else {
                ps.format(",'' u %d fs solid %.1f ti col", i + 2,
                                0.4 + i * 0.2);
            }
        }
        ps.close();
        Rt.runAndShowCommand("gnuplot " + name + ".plt", dir);
//        Rt.runAndShowCommand("gnuplot " + name + ".plt");
    }

}
