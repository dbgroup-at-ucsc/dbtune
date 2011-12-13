package edu.ucsc.dbtune.util;

/**
 * monitor execution time.
 * @author hsanchez@cs.ucsc.edu (Huascar A. Sanchez)
 */
public class StopWatch
{
    private long start = System.nanoTime();

    /**
     * @return
     *      returns elapsed time in nanoseconds then resets the watch.
     */
    public long nanoseconds()
    {
        long now = System.nanoTime();
        try {
          return now - start;
        } finally {
          start = now;
        }
    }


    /**
     * @return
     *   returns elapsed time in milliseconds then resets the watch.
     */
    public double milliseconds()
    {
        return normalize(nanoseconds(), 1000000.0);
    }

    /**
     * emits the time (in nanoseconds) that has passed; no reset is performed.
     * @return
     *      the time (in nanoseconds) that has passed; no reset is performed.
     */
    public long elapsedtime()
    {
        return System.nanoTime() - start;
    }

    /**
     * normalize elapsed time.
     *
     * @param elapsedtime
     *      now time - start time.
     * @param overheadFactor
     *      factor
     * @return
     *      the execution time's overhead.
     */
    public static double normalize(double elapsedtime, double overheadFactor)
    {
        return elapsedtime / overheadFactor;
    }

    /**
     * resets and logs elapsed time in milliseconds.
     * @param label
     *      message label.
     */
    public void resetAndLog(String label)
    {
        milliseconds();
    }
}
