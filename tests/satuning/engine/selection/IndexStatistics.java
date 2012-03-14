package satuning.engine.selection;

import java.util.HashMap;
import java.util.Map;

import satuning.Configuration;
import satuning.db.DB2Index;
import satuning.engine.ProfiledQuery;

public class IndexStatistics implements BenefitFunction, DoiFunction {
	double currentTimeStamp = 0;
	Map<DB2Index,Window> benefitWindows = new HashMap<DB2Index,Window>();
	Map<DB2IndexPair,Window> doiWindows = new HashMap<DB2IndexPair,Window>();
	DB2IndexPair tempPair = new DB2IndexPair(null, null); // for lookups
	
	public IndexStatistics() {
	}

	public void addQuery(ProfiledQuery qinfo, DynamicIndexSet matSet) {
		Iterable<DB2Index> candSet = qinfo.candidateSet;
		for (DB2Index index : candSet) {
			double bestBenefit = qinfo.bank.bestBenefit(index.internalId()) 
								    - qinfo.explainInfo.maintenanceCost(index);
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
		for (DB2Index a : candSet) {
			int id1 = a.internalId();
			for (DB2Index b : candSet) {
				int id2 = b.internalId();
				if (id1 >= id2)
					continue;
				
				double doi = qinfo.bank.interactionLevel(id1,id2); 
				if (doi != 0) {
					// add measurement, creating new window if necessary
					tempPair.a = a; tempPair.b = b;
					
					Window doiwin = doiWindows.get(tempPair);
					if (doiwin == null) {
						doiwin = new Window();
						doiWindows.put(new DB2IndexPair(a,b), doiwin);
					}
					doiwin.put(doi, currentTimeStamp);
					tempPair.a = null; tempPair.b = null;
				}
			}
		}
		
		double executionCost = qinfo.cost(matSet.bitSet());
		currentTimeStamp += executionCost;
	}

	public double benefit(DB2Index index) {
		if (currentTimeStamp == 0)
			return 0;

		Window window = benefitWindows.get(index);
		if (window == null)
			return 0;
		else
			return window.maxRate(currentTimeStamp);
	}
	
	public double doi(DB2Index a, DB2Index b) {
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
		private final int size = Configuration.indexStatisticsWindow;
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
	
	private class DB2IndexPair {
		DB2Index a, b;
		
		DB2IndexPair(DB2Index index1, DB2Index index2) {
			a = index1;
			b = index2;
		}
		
		@Override
		public int hashCode() {
			return a.hashCode() + b.hashCode();
		}
		
		@Override
		public boolean equals(Object other) {
			if (!(other instanceof DB2IndexPair))
				return false;
			DB2IndexPair pair = (DB2IndexPair) other;
			return (a.equals(pair.a) && b.equals(pair.b))
			    || (a.equals(pair.b) && b.equals(pair.a));
		}
	}		
}
