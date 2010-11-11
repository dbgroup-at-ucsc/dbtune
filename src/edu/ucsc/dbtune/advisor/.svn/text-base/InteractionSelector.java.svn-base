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
import edu.ucsc.dbtune.spi.core.Supplier;

import java.util.Random;

public class InteractionSelector {
	private static final int PARTITION_ITERATIONS = 10;
    private InteractionSelector(){}

    /**
     * choose an index partitions object (i.e., a {@link IndexPartitions}) that will be used for reorganizing candidates
     * part of a {@code snapshot} of a {@link edu.ucsc.dbtune.ibg.CandidatePool}.
     * @param arg
     *      a {@link InteractionSelection} object.
     * @param <I>
     *      a {@link DBIndex index} type.
     * @return a {@link IndexPartitions} object. <strong>IMPORTANT NOTE</strong>: When
     *      {@code #choosePartitions(InteractionSelection)} is called, hotSet and
     *      hotPartitions are out of sync!
     */
    public static <I extends DBIndex<I>> IndexPartitions<I> choosePartitions(InteractionSelection<I> arg){
        return choosePartitions(
                arg.getNewHotSet(),
                arg.getOldPartitions(),
                arg.getDoiFunction(),
                arg.getMaxNumStates()
        );
    }

	static <I extends DBIndex<I>> IndexPartitions<I> choosePartitions(StaticIndexSet<I> newHotSet,
                                                                      IndexPartitions<I> oldPartitions,
                                                                      StatisticsFunction<I> doiFunc,
                                                                      int maxNumStates
    ) {
		Random rand = new Random(50);
        IndexPartitions<I> bestPartitions = constructInitialGuess(newHotSet, oldPartitions);
		double bestCost = partitionCost(bestPartitions, doiFunc);
		
		for (int attempts = 0; attempts < PARTITION_ITERATIONS; attempts++) {
			IndexPartitions<I> currentPartitions = new IndexPartitions<I>(newHotSet);
			while (true) {
				double currentSubsetCount = currentPartitions.subsetCount();
				double currentStateCount = currentPartitions.wfaStateCount();
				double totalWeightSingletons = 0;
				double totalWeightOthers = 0;
				boolean foundSingletonPair = false;
				for (int s1 = 0; s1 < currentSubsetCount; s1++) {
					IndexPartitions.Subset<I> subset1 = currentPartitions.get(s1);
					double size1 = subset1.size();
					for (int s2 = s1+1; s2 < currentSubsetCount; s2++) { 
						IndexPartitions.Subset<I> subset2 = currentPartitions.get(s2);
                        InteractionWeightSupplier interactionWeight = new InteractionWeightSupplier<I>(
                                doiFunc,
                                maxNumStates
                        ).currentStateCount(currentStateCount)
                         .totalWeightSingletons(totalWeightSingletons)
                         .totalWeightOthers(totalWeightOthers)
                         .foundSingletonPair(foundSingletonPair)
                         .availableSubsetsAndSizeOfFirstOne(subset1, size1, subset2)
                       .get();
                        foundSingletonPair      = interactionWeight.isFoundSingletonPair();
                        totalWeightSingletons   = interactionWeight.getTotalWeightSingletons();
                        totalWeightOthers       = interactionWeight.getTotalWeightOthers();
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
					IndexPartitions.Subset<I> subset1 = currentPartitions.get(s1);
					double size1 = subset1.size();
					for (int s2 = s1+1; s2 < currentSubsetCount && accumWeight <= weightThreshold; s2++) { 
						IndexPartitions.Subset<I> subset2 = currentPartitions.get(s2);
                        InteractionWeightAccumulator interactionWeightAccumulator = new InteractionWeightAccumulator<I>(
                                doiFunc,
                                maxNumStates
                        ).currentStateCount(currentStateCount)
                         .foundSingletonPair(foundSingletonPair)
                         .accumWeight(accumWeight)
                         .availableSubsetsAndSizeOfFirstOne(subset1, size1, subset2)
                        .get();
                        accumWeight = interactionWeightAccumulator.getAccumWeight();
						
						if (accumWeight > weightThreshold){
                            currentPartitions.merge(s1, s2); // for loops will exit due to threshold
                        }
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

    private static <I extends DBIndex<I>> IndexPartitions<I> constructInitialGuess(StaticIndexSet<I> newHotSet, IndexPartitions<I> oldPartitions) {
        IndexPartitions<I> bestPartitions;

        // construct initial guess, which put indexes together that were previously together
        bestPartitions = new IndexPartitions<I>(newHotSet);
        for (int s = 0; s < oldPartitions.subsetCount(); s++) {
            IndexPartitions.Subset<I> subset = oldPartitions.get(s);
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
        return bestPartitions;
    }

    private static <I extends DBIndex<I>> double partitionCost(IndexPartitions<I> partitions,
                                                               StatisticsFunction<I> doiFunc
    ) {
		double cost = 0;
		for (int s1 = 0; s1 < partitions.subsetCount(); s1++) {
			IndexPartitions.Subset<I> subset1 = partitions.get(s1);
			for (int s2 = s1+1; s2 < partitions.subsetCount(); s2++) { 
				IndexPartitions.Subset<I> subset2 = partitions.get(s2);
				cost += interactionWeight(subset1, subset2, doiFunc);
			}
		}
		return cost;
	}

	private static <I extends DBIndex<I>> double interactionWeight(IndexPartitions.Subset<I> s1,
                                                                   IndexPartitions.Subset<I> s2,
                                                                   StatisticsFunction<I> doiFunc
    ) {
		double weight = 0;
		for (I i1 : s1)
			for (I i2 : s2) 
				weight += doiFunc.doi(i1, i2);
		return weight;
	}

    private static class InteractionWeightSupplier<I extends DBIndex<I>>
    implements Supplier<InteractionWeightSupplier<I>> {
        private StatisticsFunction<I> doiFunc;
        private int maxNumStates;
        private double currentStateCount;
        private double totalWeightSingletons;
        private double totalWeightOthers;
        private boolean foundSingletonPair;
        private IndexPartitions.Subset<I> subset1;
        private double size1;
        private IndexPartitions.Subset<I> subset2;

        public InteractionWeightSupplier(StatisticsFunction<I> doiFunc, int maxNumStates) {
            this.doiFunc        = doiFunc;
            this.maxNumStates   = maxNumStates;
        }

        public InteractionWeightSupplier<I> currentStateCount(double value){
            currentStateCount = value;
            return this;
        }

        public InteractionWeightSupplier<I> totalWeightSingletons(double value){
            totalWeightSingletons = value;
            return this;
        }

        public InteractionWeightSupplier<I> totalWeightOthers(double value){
            totalWeightOthers = value;
            return this;
        }

        public InteractionWeightSupplier<I> foundSingletonPair(boolean value){
            foundSingletonPair = value;
            return this;
        }

        public InteractionWeightSupplier<I> availableSubsetsAndSizeOfFirstOne(
                IndexPartitions.Subset<I> subset1,
                double size1,
                IndexPartitions.Subset<I> subset2
        ){
            this.subset1 = subset1;
            this.size1   = size1;
            this.subset2 = subset2;
            return this;
        }

        public double getTotalWeightSingletons() {
            return totalWeightSingletons;
        }

        public double getTotalWeightOthers() {
            return totalWeightOthers;
        }

        public boolean isFoundSingletonPair() {
            return foundSingletonPair;
        }

        @Override
        public InteractionWeightSupplier<I> get() {
            double size2 = subset2.size();
            double weight = interactionWeight(subset1, subset2, doiFunc);
            if (weight == 0)
                return this;

            if (size1 == 1 && size2 == 1) {
                foundSingletonPair = true;
                totalWeightSingletons += weight;
            } else if (!foundSingletonPair) {
                double addedStates = Math.pow(2, size1 +size2) - Math.pow(2, size1) - Math.pow(2, size2);
                if (addedStates + currentStateCount > maxNumStates)
                    return this;
                totalWeightOthers += weight / addedStates;
            }
            return this;
        }
    }

    private static class InteractionWeightAccumulator<I extends DBIndex<I>>
    implements Supplier<InteractionWeightAccumulator<I>> {
        private StatisticsFunction<I>     doiFunc;
        private int                       maxNumStates;
        private double                    currentStateCount;
        private boolean                   foundSingletonPair;
        private double                    accumWeight;
        private IndexPartitions.Subset<I> subset1;
        private double                    size1;
        private IndexPartitions.Subset<I> subset2;

        public InteractionWeightAccumulator(StatisticsFunction<I> doiFunc, int maxNumStates) {
            this.doiFunc        = doiFunc;
            this.maxNumStates   = maxNumStates;
        }

        public InteractionWeightAccumulator<I> currentStateCount(double value){
            currentStateCount = value;
            return this;
        }

        public InteractionWeightAccumulator<I> foundSingletonPair(boolean value){
            foundSingletonPair = value;
            return this;
        }

        public InteractionWeightAccumulator<I> accumWeight(double value){
            accumWeight = value;
            return this;
        }

        public InteractionWeightAccumulator<I> availableSubsetsAndSizeOfFirstOne(
                IndexPartitions.Subset<I> subset1,
                double size1,
                IndexPartitions.Subset<I> subset2
        ){
            this.subset1 = subset1;
            this.size1   = size1;
            this.subset2 = subset2;
            return this;
        }

        public double getAccumWeight() {
            return accumWeight;
        }

        @Override
        public InteractionWeightAccumulator<I> get() {
            double size2 = subset2.size();
            double weight = interactionWeight(subset1, subset2, doiFunc);
            if (weight == 0)
                return this;

            if (size1 == 1 && size2 == 1) {
                accumWeight += weight;
            }
            else if (!foundSingletonPair) {
                double addedStates = Math.pow(2, size1 +size2) - Math.pow(2, size1) - Math.pow(2, size2);
                if (addedStates + currentStateCount > maxNumStates)
                    return this;
                accumWeight += weight / addedStates;
            }
            return this;
        }
    }
}
