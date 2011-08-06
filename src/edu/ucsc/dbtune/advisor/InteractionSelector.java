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
    private InteractionSelector(){}

    /**
     * choose an index partitions object (i.e., a {@link IndexPartitions}) that will be used for reorganizing candidates
     * part of a {@code snapshot} of a {@link edu.ucsc.dbtune.ibg.CandidatePool}.
     * @param arg
     *      a {@link InteractionSelection} object.
	 * @return a {@link IndexPartitions} object. <strong>IMPORTANT NOTE</strong>: When
     *      {@code #choosePartitions(InteractionSelection)} is called, hotSet and
     *      hotPartitions are out of sync!
     */
    public static IndexPartitions choosePartitions(
            InteractionSelection arg,
            int partitionIterations)
    {
        return choosePartitions(
                arg.getNewHotSet(),
                arg.getOldPartitions(),
                arg.getDoiFunction(),
                arg.getMaxNumStates(),
                partitionIterations );
    }

    static IndexPartitions choosePartitions(
            StaticIndexSet     newHotSet,
            IndexPartitions    oldPartitions,
            StatisticsFunction doiFunc,
            int                   maxNumStates,
            int                   partitionIterations)
    {
        Random rand = new Random(50);
        IndexPartitions bestPartitions = constructInitialGuess(newHotSet, oldPartitions);
        double bestCost = partitionCost(bestPartitions, doiFunc);

        for (int attempts = 0; attempts < partitionIterations; attempts++) {
            IndexPartitions currentPartitions = new IndexPartitions(newHotSet);
            while (true) {
                double currentSubsetCount = currentPartitions.subsetCount();
                double currentStateCount = currentPartitions.wfaStateCount();
                double totalWeightSingletons = 0;
                double totalWeightOthers = 0;
                boolean foundSingletonPair = false;
                for (int s1 = 0; s1 < currentSubsetCount; s1++) {
                    IndexPartitions.Subset subset1 = currentPartitions.get(s1);
                    double size1 = subset1.size();
                    for (int s2 = s1+1; s2 < currentSubsetCount; s2++) { 
                        IndexPartitions.Subset subset2 = currentPartitions.get(s2);
                        InteractionWeightSupplier interactionWeight = new 
                            InteractionWeightSupplier(
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
                    IndexPartitions.Subset subset1 = currentPartitions.get(s1);
                    double size1 = subset1.size();
                    for (int s2 = s1+1; s2 < currentSubsetCount && accumWeight <= weightThreshold; s2++) { 
                        IndexPartitions.Subset subset2 = currentPartitions.get(s2);
                        InteractionWeightAccumulator interactionWeightAccumulator = new 
                            InteractionWeightAccumulator(
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

    private static IndexPartitions constructInitialGuess(StaticIndexSet newHotSet, IndexPartitions oldPartitions) {
        IndexPartitions bestPartitions;

        // construct initial guess, which put indexes together that were previously together
        bestPartitions = new IndexPartitions(newHotSet);
        for (int s = 0; s < oldPartitions.subsetCount(); s++) {
            IndexPartitions.Subset subset = oldPartitions.get(s);
            for (DBIndex i1 : subset) {
                if (!newHotSet.contains(i1))
                    continue;
                for (DBIndex i2 : subset) {
                    if (i1 == i2 || !newHotSet.contains(i2))
                        continue;
                    bestPartitions.merge(i1, i2);
                }
            }
        }
        return bestPartitions;
    }

    private static double partitionCost(IndexPartitions partitions,
                                                               StatisticsFunction doiFunc
    ) {
        double cost = 0;
        for (int s1 = 0; s1 < partitions.subsetCount(); s1++) {
            IndexPartitions.Subset subset1 = partitions.get(s1);
            for (int s2 = s1+1; s2 < partitions.subsetCount(); s2++) { 
                IndexPartitions.Subset subset2 = partitions.get(s2);
                cost += interactionWeight(subset1, subset2, doiFunc);
            }
        }
        return cost;
    }

    private static double interactionWeight(IndexPartitions.Subset s1,
                                                                   IndexPartitions.Subset s2,
                                                                   StatisticsFunction doiFunc
    ) {
        double weight = 0;
        for (DBIndex i1 : s1)
            for (DBIndex i2 : s2) 
                weight += doiFunc.doi(i1, i2);
        return weight;
    }

    private static class InteractionWeightSupplier
    implements Supplier<InteractionWeightSupplier> {
        private StatisticsFunction doiFunc;
        private int maxNumStates;
        private double currentStateCount;
        private double totalWeightSingletons;
        private double totalWeightOthers;
        private boolean foundSingletonPair;
        private IndexPartitions.Subset subset1;
        private double size1;
        private IndexPartitions.Subset subset2;

        public InteractionWeightSupplier(StatisticsFunction doiFunc, int maxNumStates) {
            this.doiFunc        = doiFunc;
            this.maxNumStates   = maxNumStates;
        }

        public InteractionWeightSupplier currentStateCount(double value){
            currentStateCount = value;
            return this;
        }

        public InteractionWeightSupplier totalWeightSingletons(double value){
            totalWeightSingletons = value;
            return this;
        }

        public InteractionWeightSupplier totalWeightOthers(double value){
            totalWeightOthers = value;
            return this;
        }

        public InteractionWeightSupplier foundSingletonPair(boolean value){
            foundSingletonPair = value;
            return this;
        }

        public InteractionWeightSupplier availableSubsetsAndSizeOfFirstOne(
                IndexPartitions.Subset subset1,
                double size1,
                IndexPartitions.Subset subset2
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
        public InteractionWeightSupplier get() {
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

    private static class InteractionWeightAccumulator
    implements Supplier<InteractionWeightAccumulator> {
        private StatisticsFunction     doiFunc;
        private int                       maxNumStates;
        private double                    currentStateCount;
        private boolean                   foundSingletonPair;
        private double                    accumWeight;
        private IndexPartitions.Subset subset1;
        private double                    size1;
        private IndexPartitions.Subset subset2;

        public InteractionWeightAccumulator(StatisticsFunction doiFunc, int maxNumStates) {
            this.doiFunc        = doiFunc;
            this.maxNumStates   = maxNumStates;
        }

        public InteractionWeightAccumulator currentStateCount(double value){
            currentStateCount = value;
            return this;
        }

        public InteractionWeightAccumulator foundSingletonPair(boolean value){
            foundSingletonPair = value;
            return this;
        }

        public InteractionWeightAccumulator accumWeight(double value){
            accumWeight = value;
            return this;
        }

        public InteractionWeightAccumulator availableSubsetsAndSizeOfFirstOne(
                IndexPartitions.Subset subset1,
                double size1,
                IndexPartitions.Subset subset2
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
        public InteractionWeightAccumulator get() {
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
