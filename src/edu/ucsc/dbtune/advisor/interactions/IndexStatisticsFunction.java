package edu.ucsc.dbtune.advisor.interactions;

import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.util.BitArraySet;
import edu.ucsc.dbtune.util.ToStringBuilder;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * @author Huascar A. Sanchez
 * @author Ivo Jimenez
 */
public class IndexStatisticsFunction
{
    private Map<IndexPair, MeasurementWindow> doiWindows;
    private Map<Index, MeasurementWindow>     benefitWindows;
    private double          currentTimeStamp;
    private IndexPair       tempPair;
    private DoiFunction     doi;
    private BenefitFunction benefit;

    /**
     * Construct an {@code IndexStatistics} object. This object collects measurements that
     * correspond to either doi or benefit measurements at some point in time.
     *
     * @param indexStatisticsWindowSize
     *      window size
     */
    public IndexStatisticsFunction(int indexStatisticsWindowSize)
    {
        this.tempPair              = IndexPair.emptyPair();
        this.doiWindows            = new HashMap<IndexPair,MeasurementWindow>();
        this.benefitWindows        = new HashMap<Index,MeasurementWindow>();
        this.currentTimeStamp      = 0;
        this.doi                   = new DoiFunction(this);
        this.benefit               = new BenefitFunction(this);
    }

    /*
    public void addQuery(IBGPreparedSQLStatement queryInfo, Configuration matSet) throws SQLException
    {
        Configuration conf = queryInfo.getConfiguration();

        for (Index index : conf) {
            final InteractionBank bank = queryInfo.getInteractionBank();

            double bestBenefit = bank.bestBenefit(conf.getOrdinalPosition(index))
                                 - queryInfo.getUpdateCost(index);
            if (bestBenefit != 0) {
                // add measurement, creating new window if necessary
                MeasurementWindow benwin = benefitWindows.get(index);
                if (benwin == null) {
                    benwin = new MeasurementWindow(indexStatisticsWindow);
                    benefitWindows.put(index, benwin);
                }
                benwin.put(bestBenefit, currentTimeStamp);
            }
        }

        calculateInteractionLevel(queryInfo, matSet, queryInfo.getConfiguration());
    }

    private void calculateInteractionLevel(
            IBGPreparedSQLStatement queryInfo,
            Configuration matSet,
            Configuration candSet
    ) throws SQLException {
        // not the most efficient double loop, but an ok compromise for now
        for (Index a : candSet) {
            int id1 = candSet.getOrdinalPosition(a);
            for (Index b : candSet) {
                int id2 = candSet.getOrdinalPosition(b);
                if (id1 >= id2){
                    continue;
                }

                final InteractionBank bank = queryInfo.getInteractionBank();
                double doi = bank.interactionLevel(id1,id2);
                if (doi != 0) {
                    addsMeasurement(a, b, doi);
                }
            }
        }

        Configuration bsConf = new Configuration(matSet);
        currentTimeStamp += queryInfo.explain(bsConf).getTotalCost();
    }

    private void addsMeasurement(Index a, Index b, double doi)
    {
        // add measurement, creating new window if necessary
        updateIndexPairs(a, b);
        MeasurementWindow doiwin = doiWindows.get(tempPair);
        if (doiwin == null) {
            doiwin = new MeasurementWindow(indexStatisticsWindow);
            doiWindows.put(tempPair, doiwin);
        }
        doiwin.put(doi, currentTimeStamp);
        clearIndexPairs();
    }
    */

    private void clearIndexPairs()
    {        
        tempPair.a = null; tempPair.b = null;
    }

    private void updateIndexPairs(Index a, Index b)
    {
        tempPair.a = a; tempPair.b = b;
    }

    public double doi(Index a, Index b)
    {
        return doi.apply(a, b) ;
    }

    public double benefit(Index a, BitArraySet<Index> m)
    {
        return benefit.apply(a, m);
    }

    /**
     * Maintains a sliding window of measurements
     * This class is agnostic about what the measurements indicate, and just treats them as numbers
     *
     * The most recent measurement is stored in measurements[lastPos] and has
     * timestamp stored in timestamps[lastPos]. The older measurements are
     * stored in (lastPos+1)%size, (lastPos+2)%size etc, until a position i is
     * encountered such that timestamps[i] == -1. The number of measurements is
     * indicated by the field numMeasurements.
     */
    public static class MeasurementWindow
    {
        private int size;

        double[] measurements = new double[size];
        double[] timestamps   = new double[size];
        int lastPos = -1;
        int numMeasurements = 0;

        MeasurementWindow(int indexStatisticsWindow)
        {
            size = indexStatisticsWindow;
        }

        /**
         * records a measurement in time.
         * @param meas
         *      measurement value.
         * @param time
         *      the timestamp.
         */
        void put(double meas, double time) {
            if (numMeasurements < size) {
                ++numMeasurements;
                lastPos = size-numMeasurements;
            }
            else if (lastPos == 0) {
                lastPos = size - 1;
            }
            else {
                --lastPos;
            }

            measurements[lastPos] = meas;
            timestamps[lastPos] = time;
        }

        /**
         * Main computation supported by this data structure:
         * Find the maximum of
         *   sum(measurements) / sum(time)
         * over all suffixes of the window.
         * @param time
         *      time window.
         * @return zero if no measurements have been made.
         */
        double maxRate(double time) {
            if (numMeasurements == 0)
                return 0;

            double sumMeasurements = measurements[lastPos];
            double maxRate = sumMeasurements / (time - timestamps[lastPos]);
            for (int measNum = 1; measNum < numMeasurements; measNum++) {
                int i = measNum % size;
                sumMeasurements += measurements[i];
                double rate = sumMeasurements / (time - timestamps[i]);
                maxRate = Math.max(rate, maxRate);
            }

            return maxRate;

        }

        @Override
        public String toString()
        {
            return new ToStringBuilder<MeasurementWindow>(this)
                   .add("measurements", Arrays.toString(measurements))
                   .add("timestamps", Arrays.toString(timestamps))
                   .add("lastPos", lastPos)
                   .add("numMeasurements", numMeasurements)
                   .toString();
        }
    }


    private static class DoiFunction
    {
        private final IndexStatisticsFunction statistics;

        DoiFunction(IndexStatisticsFunction statistics){
            this.statistics = statistics;
        }

        public double apply(Index a, Index b)
        {
            if (statistics.currentTimeStamp == 0)
                return 0;

            statistics.updateIndexPairs(a, b);
            final MeasurementWindow window = statistics.doiWindows.get(statistics.tempPair);
            statistics.clearIndexPairs();
            if (window == null){
                return 0;
            } else {
                return window.maxRate(statistics.currentTimeStamp);
            }
        }
    }

    private static class BenefitFunction
    {
        private final IndexStatisticsFunction statistics;

        public BenefitFunction(IndexStatisticsFunction statistics)
        {
            this.statistics = statistics;
        }

        public double apply(Index arg, BitArraySet<Index> m)
        {
            if (statistics.currentTimeStamp == 0)
                return 0;
            final MeasurementWindow window = statistics.benefitWindows.get(arg);
            if (window == null){
                return 0;
            } else {
                return window.maxRate(statistics.currentTimeStamp);
            }
        }
    }

    static class IndexPair {
        Index a;
        Index b;

        IndexPair(Index a, Index b) {
            this.a = a;
            this.b = b;
        }

        static IndexPair emptyPair(){
            return of(null, null);
        }

        static IndexPair of(Index a, Index b){
            return new IndexPair(a, b);
        }

        @Override
        public int hashCode()
        {
            return a.hashCode() + b.hashCode();
        }


        @Override
        public boolean equals(Object other)
        {
            if (!(other instanceof IndexPair)){
                return false;
            }

            final IndexPair pair = (IndexPair) other;
            return (a.equals(pair.a) && b.equals(pair.b))
                || (a.equals(pair.b) && b.equals(pair.a));
        }

        @Override
        public String toString()
        {
            return new ToStringBuilder<IndexPair>(this)
                   .add("left", a)
                   .add("right", b)
                   .toString();
        }
    }
}
