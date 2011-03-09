/*
 * ****************************************************************************
 *   Copyright 2010 University of California Santa Cruz                       *
 *                                                                            *
 *   Licensed under the Apache License, Version 2.0 (the "License");          *
 *   you may not use this file except in compliance with the License.         *
 *   You may obtain a copy of the License at                                  *
 *                                                                            *
 *       http://www.apache.org/licenses/LICENSE-2.0                           *
 *                                                                            *
 *   Unless required by applicable law or agreed to in writing, software      *
 *   distributed under the License is distributed on an "AS IS" BASIS,        *
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. *
 *   See the License for the specific language governing permissions and      *
 *   limitations under the License.                                           *
 *  ****************************************************************************
 */
package edu.ucsc.dbtune.advisor;

import edu.ucsc.dbtune.core.DBIndex;
import edu.ucsc.dbtune.core.ExplainInfo;
import edu.ucsc.dbtune.util.IndexBitSet;
import edu.ucsc.dbtune.util.Instances;
import edu.ucsc.dbtune.util.ToStringBuilder;
import edu.ucsc.dbtune.ibg.InteractionBank;

import java.util.Arrays;
import java.util.Map;

/**
 * @author huascar.sanchez@gmail.com (Huascar A. Sanchez)
 */
public class IndexStatisticsFunction<I extends DBIndex> implements StatisticsFunction<I> {
    private static final int INDEX_STATISTICS_WINDOW = 100;

    double        currentTimeStamp;
    private       DoiFunction<I>     doi;
    private       BenefitFunction<I> benefit;
    private       DBIndexPair<I>     tempPair;

    private final Map<DBIndexPair<I>, MeasurementWindow>    doiWindows;
    private final Map<I, MeasurementWindow>                 benefitWindows;

    /**
     * Construct an {@code IndexStatistics} object.
     */
    public IndexStatisticsFunction(){
        this(
                DBIndexPair.<I>emptyPair(),
                Instances.<DBIndexPair<I>, MeasurementWindow>newHashMap(),
                Instances.<I, MeasurementWindow>newHashMap()

        );
    }

    /**
     * Construct an {@code IndexStatistics} object. This object collects measurements that
     * correspond to either doi or benefit measurements at some point in time (i.e.,
     * {@link MeasurementWindow measurement window}).
     * @param indexPair
     *      a pair of {@link DBIndex indexes} used for lookups.
     * @param doiWindows
     *      a map of {@link DBIndexPair indexes pair} to {@link MeasurementWindow window}.
     * @param benefitWindows
     *      a map of {@link DBIndex index} to {@link MeasurementWindow window}.
     */
    public IndexStatisticsFunction(
            DBIndexPair<I> indexPair,
            Map<DBIndexPair<I>, MeasurementWindow> doiWindows,
            Map<I, MeasurementWindow> benefitWindows
    ){
        this.tempPair           = indexPair;
        this.doiWindows         = doiWindows;
        this.benefitWindows     = benefitWindows;
        this.currentTimeStamp   = 0;

        bindIndexStatisticsWithDoiAndBenefit();
    }

    private void bindIndexStatisticsWithDoiAndBenefit(){
        this.doi                = new DoiFunctionImpl<I>(this);
        this.benefit            = new BenefitFunctionImpl<I>(this);
    }

    @Override
    public void addQuery(ProfiledQuery<I> queryInfo, DynamicIndexSet<?> matSet) {
        Iterable<I> candSet = queryInfo.getCandidateSnapshot();
        for (I index : candSet) {
            final InteractionBank bank          = queryInfo.getInteractionBank();
            final ExplainInfo explainInfo   = queryInfo.getExplainInfo();

            double bestBenefit = bank.bestBenefit(index.internalId())
                                 - explainInfo.getIndexMaintenanceCost(index);
            if (bestBenefit != 0) {
                // add measurement, creating new window if necessary
                MeasurementWindow benwin = benefitWindows.get(index);
                if (benwin == null) {
                    benwin = new MeasurementWindow();
                    benefitWindows.put(index, benwin);
                }
                benwin.put(bestBenefit, currentTimeStamp);
            }
        }

        calculateInteractionLevel(queryInfo, matSet, candSet);
    }

    private void calculateInteractionLevel(
            ProfiledQuery<I> queryInfo,
            DynamicIndexSet<?> matSet,
            Iterable<I> candSet
    ) {
        // not the most efficient double loop, but an ok compromise for now
        for (I a : candSet) {
            int id1 = a.internalId();
            for (I b : candSet) {
                int id2 = b.internalId();
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

        double executionCost = queryInfo.totalCost(matSet.bitSet());
        currentTimeStamp += executionCost;
    }

    private void clearIndexPairs(){        
        tempPair.a = null; tempPair.b = null;
    }

    private void updateIndexPairs(I a, I b){
        tempPair.a = a; tempPair.b = b;
    }

    private void addsMeasurement(I a, I b, double doi) {
        // add measurement, creating new window if necessary
        updateIndexPairs(a, b);
        MeasurementWindow doiwin = doiWindows.get(tempPair);
        if (doiwin == null) {
            doiwin = new MeasurementWindow();
            doiWindows.put(tempPair, doiwin);
        }
        doiwin.put(doi, currentTimeStamp);
        clearIndexPairs();
    }

    @Override
    public double doi(I a, I b) {
        return doi.apply(a, b) ;
    }

    @Override
    public double benefit(I a, IndexBitSet m) {
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
    static class MeasurementWindow {
        private final int size = INDEX_STATISTICS_WINDOW;

        double[] measurements = new double[size];
        double[] timestamps   = new double[size];
        int lastPos = -1;
        int numMeasurements = 0;

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
        public String toString() {
            return new ToStringBuilder<MeasurementWindow>(this)
                   .add("measurements", Arrays.toString(measurements))
                   .add("timestamps", Arrays.toString(timestamps))
                   .add("lastPos", lastPos)
                   .add("numMeasurements", numMeasurements)
                   .toString();
        }
    }


    private static class DoiFunctionImpl<I extends DBIndex> implements DoiFunction<I> {
        private final IndexStatisticsFunction<I> statistics;

        DoiFunctionImpl(IndexStatisticsFunction<I> statistics){
            this.statistics = statistics;
        }

        @Override
        public double apply(I a, I b) {
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

    private static class BenefitFunctionImpl <I extends DBIndex> implements BenefitFunction<I> {
        private final IndexStatisticsFunction<I> statistics;

        BenefitFunctionImpl(IndexStatisticsFunction<I> statistics){
            this.statistics = statistics;
        }

        @Override
        public double apply(I arg, IndexBitSet m) {
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

    static class DBIndexPair<I extends DBIndex> {
        I a;
        I b;

        DBIndexPair(I a, I b) {
            this.a = a;
            this.b = b;
        }

        static <I extends DBIndex> DBIndexPair<I> emptyPair(){
            return of(null, null);
        }

        static <I extends DBIndex> DBIndexPair<I> of(I a, I b){
            return new DBIndexPair<I>(a, b);
        }

        @Override
        public int hashCode() {
            return a.hashCode() + b.hashCode();
        }


        @Override
        public boolean equals(Object other) {
            if (!(other instanceof DBIndexPair)){
                return false;
            }

            final DBIndexPair<?> pair = (DBIndexPair<?>) other;
            return (a.equals(pair.a) && b.equals(pair.b))
                || (a.equals(pair.b) && b.equals(pair.a));
        }

        @Override
        public String toString() {
            return new ToStringBuilder<DBIndexPair<I>>(this)
                   .add("left", a)
                   .add("right", b)
                   .toString();
        }
    }

}
