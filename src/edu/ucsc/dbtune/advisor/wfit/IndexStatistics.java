package edu.ucsc.dbtune.advisor.wfit;

import java.util.HashMap;
import java.util.Map;

import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.util.Environment;

public class IndexStatistics implements BenefitFunction, DoiFunction {
    double currentTimeStamp = 0;
    Map<Index,Window> benefitWindows = new HashMap<Index,Window>();
    Map<IndexPair,Window> doiWindows = new HashMap<IndexPair,Window>();
    IndexPair tempPair = new IndexPair(null, null); // for lookups
    
    public IndexStatistics() {
    }

    public void addQuery(ProfiledQuery qinfo, DynamicIndexSet matSet) {
        Iterable<Index> candSet = qinfo.candidateSet;
        for (Index index : candSet) {
            double bestBenefit = qinfo.bank.bestBenefit(index) 
                                    - qinfo.explainInfo.getUpdateCost(index);
            if (bestBenefit != 0) {
                // add measurement, creating new window if necessary
                Window benwin = benefitWindows.get(index);
                if (benwin == null) {
                    benwin = new Window();
                    benefitWindows.put(index, benwin);
                }
                benwin.put(bestBenefit, currentTimeStamp);
            }
        }
        
        // not the most efficient double loop, but an ok compromise for now
        for (Index a : candSet) {
            int id1 = a.getId();
            for (Index b : candSet) {
                int id2 = b.getId();
                if (id1 >= id2)
                    continue;
                
                double doi = qinfo.bank.interactionLevel(a, b);

                if (doi != 0) {
                    // add measurement, creating new window if necessary
                    tempPair.a = a; tempPair.b = b;
                    
                    Window doiwin = doiWindows.get(tempPair);
                    if (doiwin == null) {
                        doiwin = new Window();
                        doiWindows.put(new IndexPair(a,b), doiwin);
                    }
                    doiwin.put(doi, currentTimeStamp);
                    tempPair.a = null; tempPair.b = null;
                }
            }
        }
        
        double executionCost = qinfo.cost(matSet.bitSet());
        currentTimeStamp += executionCost;
    }

    @Override
    public double benefit(Index index) {
        if (currentTimeStamp == 0)
            return 0;

        Window window = benefitWindows.get(index);
        if (window == null)
            return 0;
        else
            return window.maxRate(currentTimeStamp);
    }
    
    @Override
    public double doi(Index a, Index b) {
        if (currentTimeStamp == 0)
            return 0;

        tempPair.a = a; tempPair.b = b;
        Window window = doiWindows.get(tempPair);
        tempPair.a = null; tempPair.b = null;
        
        if (window == null)
            return 0;
        else 
            return window.maxRate(currentTimeStamp);
    }
    
    /*
     * Maintains a sliding window of measurements
     * This class is agnostic about what the measurements indicate, and just treats them as numbers
     * 
     * The most recent measurement is stored in measurements[lastPos] and has
     * timestamp stored in timestamps[lastPos]. The older measurements are
     * stored in (lastPos+1)%size, (lastPos+2)%size etc, until a position i is
     * encountered such that timestamps[i] == -1. The number of measurements is
     * indicated by the field numMeasurements.
     * 
     */
    private class Window {
        private final int size = Environment.getInstance().getIndexStatisticsWindow();
        double[] measurements = new double[size];
        double[] timestamps = new double[size];
        int lastPos = -1;
        int numMeasurements = 0;
        
        Window() {
        }
        
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
        
        /*
         * Main computation supported by this data structure:
         * Find the maximum of 
         *   sum(measurements) / sum(time)
         * over all suffixes of the window.
         * Return zero if no measurements have been made.
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
    }
    
    private class IndexPair {
        Index a, b;
        
        IndexPair(Index index1, Index index2) {
            a = index1;
            b = index2;
        }
        
        @Override
        public int hashCode() {
            return a.hashCode() + b.hashCode();
        }
        
        @Override
        public boolean equals(Object other) {
            if (!(other instanceof IndexPair))
                return false;
            IndexPair pair = (IndexPair) other;
            return (a.equals(pair.a) && b.equals(pair.b))
                || (a.equals(pair.b) && b.equals(pair.a));
        }
    }       
}
