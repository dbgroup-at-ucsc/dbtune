package edu.ucsc.satuning.engine.selection;

import edu.ucsc.satuning.Configuration;
import edu.ucsc.satuning.db.DBIndex;

public class InteractionSelector {
	private static final int PARTITION_ITERATIONS = Configuration.partitionIterations;
	
	/*
	 * Note that when this is called, hotSet and hotPartitions are out of sync!
	 */
	public static <I extends DBIndex<I>> IndexPartitions<I> 
	choosePartitions(StaticIndexSet<I> newHotSet, IndexPartitions<I> oldPartitions,
					 DoiFunction<I> doiFunc, int maxNumStates) {
		java.util.Random rand = new java.util.Random(50);
		IndexPartitions<I> bestPartitions;
		double bestCost;
		
		/* construct initial guess, which put indexes together that were previously together */
		bestPartitions = new IndexPartitions<I>(newHotSet);
		for (int s = 0; s < oldPartitions.subsetCount(); s++) {
			IndexPartitions<I>.Subset subset = oldPartitions.get(s);
			for (I i1 : subset) {
				if (!newHotSet.contains(i1))
					continue;
				for (I i2 : subset) {
					if (i1 == i2 || !newHotSet.contains(i2))
						continue;
					bestPartitions.merge(i1, i2);
				}
			}
		}
		bestCost = partitionCost(bestPartitions, doiFunc);
		
		for (int attempts = 0; attempts < PARTITION_ITERATIONS; attempts++) {
			IndexPartitions<I> currentPartitions = new IndexPartitions<I>(newHotSet);
			while (true) {
				double currentSubsetCount = currentPartitions.subsetCount();
				double currentStateCount = currentPartitions.wfaStateCount();
				double totalWeightSingletons = 0;
				double totalWeightOthers = 0;
				boolean foundSingletonPair = false;
				for (int s1 = 0; s1 < currentSubsetCount; s1++) {
					IndexPartitions<I>.Subset subset1 = currentPartitions.get(s1);
					double size1 = subset1.size();
					for (int s2 = s1+1; s2 < currentSubsetCount; s2++) { 
						IndexPartitions<I>.Subset subset2 = currentPartitions.get(s2);
						double size2 = subset2.size();
						double weight = interactionWeight(subset1, subset2, doiFunc);
						if (weight == 0)
							continue;
						
						if (size1 == 1 && size2 == 1) {
							foundSingletonPair = true;
							totalWeightSingletons += weight;
						}
						else if (!foundSingletonPair) {
							double addedStates = Math.pow(2, size1+size2) - Math.pow(2, size1) - Math.pow(2, size2);
							if (addedStates + currentStateCount > maxNumStates)
								continue;
							totalWeightOthers += weight / addedStates;
						}
					}
				}
				
				double weightThreshold;
				if (foundSingletonPair)
					weightThreshold = rand.nextDouble() * totalWeightSingletons;
				else if (totalWeightOthers > 0)
					weightThreshold = rand.nextDouble() * totalWeightOthers;
				else
					break;
				
				double accumWeight = 0;
				for (int s1 = 0; s1 < currentSubsetCount && accumWeight <= weightThreshold; s1++) {
					IndexPartitions<I>.Subset subset1 = currentPartitions.get(s1);
					double size1 = subset1.size();
					for (int s2 = s1+1; s2 < currentSubsetCount && accumWeight <= weightThreshold; s2++) { 
						IndexPartitions<I>.Subset subset2 = currentPartitions.get(s2);
						double size2 = subset2.size();
						double weight = interactionWeight(subset1, subset2, doiFunc);
						if (weight == 0)
							continue;
						
						if (size1 == 1 && size2 == 1) {
							accumWeight += weight;
						}
						else if (!foundSingletonPair) {
							double addedStates = Math.pow(2, size1+size2) - Math.pow(2, size1) - Math.pow(2, size2);
							if (addedStates + currentStateCount > maxNumStates)
								continue;
							accumWeight += weight / addedStates;
						}
						
						if (accumWeight > weightThreshold) 
							currentPartitions.merge(s1, s2); // for loops will exit due to threshold
					}
				}
			} // end of while(true)
			
			// currentPartitions is our new candidate, now compare it
			double currentCost = partitionCost(currentPartitions, doiFunc);
			if (currentCost < bestCost) { 
				bestCost = currentCost;
				bestPartitions = currentPartitions;
			}
		}
		
		return bestPartitions;
	}
	
	private static <I extends DBIndex<I>> double 
	partitionCost(IndexPartitions<I> partitions, DoiFunction<I> doiFunc) {
		double cost = 0;
		for (int s1 = 0; s1 < partitions.subsetCount(); s1++) {
			IndexPartitions<I>.Subset subset1 = partitions.get(s1);
			for (int s2 = s1+1; s2 < partitions.subsetCount(); s2++) { 
				IndexPartitions<I>.Subset subset2 = partitions.get(s2);
				cost += interactionWeight(subset1, subset2, doiFunc);
			}
		}
		return cost;
	}

	private static <I extends DBIndex<I>> double 
	interactionWeight(IndexPartitions<I>.Subset s1, IndexPartitions<I>.Subset s2, DoiFunction<I> doiFunc) {
		double weight = 0;
		for (I i1 : s1)
			for (I i2 : s2) 
				weight += doiFunc.doi(i1, i2);
		return weight;
	}
}
