package edu.ucsc.dbtune.seq.utils;

public class RTimerN {
    long start0;
    long start;

    public RTimerN() {
        start = start0 = System.nanoTime();
    }

    public void reset() {
        start0 = start = System.nanoTime();
    }

    public long get() {
        return System.nanoTime() - start0;
    }

    public double getSecondElapse() {
        return get() / 1000000000.0;
    }
}
