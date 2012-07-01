package edu.ucsc.dbtune.seq.utils;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintStream;
import java.util.Hashtable;
import java.util.Vector;

public class PerfTest {
    static RTimerN timer = new RTimerN();
    static Hashtable<String, Vector<Long>> hash = new Hashtable<String, Vector<Long>>();

    public static void startTimer() {
        timer.reset();
    }

    public static void endTimer(String name) {
        Vector<Long> times = hash.get(name);
        if (times == null) {
            times = new Vector<Long>();
            hash.put(name, times);
        }
        times.add(timer.get());
        timer.reset();
    }

    public static void addTimer(String name) {
        Vector<Long> times = hash.get(name);
        if (times == null) {
            times = new Vector<Long>();
            hash.put(name, times);
        }
        if (times.size() == 0)
            times.add(timer.get());
        else
            times.set(0, times.get(0) + timer.get());
        timer.reset();
    }

    public static void report(File file) throws IOException {
        PrintStream ps = new PrintStream(file);
        for (String name : Rt.sort(hash.keySet())) {
            ps.print(name + "\t");
            Vector<Long> times = hash.get(name);
            for (long l : times) {
                ps.format("%.2f ", l / 1000000000.0);
            }
            ps.println();
        }
        ps.close();
    }
}
