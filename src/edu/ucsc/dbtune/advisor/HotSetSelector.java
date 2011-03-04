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
import edu.ucsc.dbtune.ibg.CandidatePool;
import edu.ucsc.dbtune.util.IndexBitSet;
import edu.ucsc.dbtune.util.MinQueue;

public class HotSetSelector {
    private HotSetSelector(){}

    /**
     * Choose a hot set (i.e., a {@link StaticIndexSet}) that will be used for reorganizing 
     * candidates part of a {@code snapshot} of a {@link CandidatePool}.
     *
     * @param arg
     *      a hot {@link HotsetSelection selection var} which contains specific values that will
     *      be utilized during the hot set selection process.
     * @param <I>
     *      the {@link DBIndex} type.
     * @return
     *      a hot set (i.e., a {@link StaticIndexSet}) 
     */
    public static <I extends DBIndex> StaticIndexSet<I> chooseHotSet(HotsetSelection<I> arg){
        return chooseHotSet(
                arg.getCandidateSet(),
                arg.getOldHotSet(),
                arg.getRequiredIndexSet(),
                arg.getBenefitFunction(),
                arg.getMaxSize(),
                arg.isDebugOutputEnabled()
        );
    }

    /**
     * choose a hot set (i.e., a {@link StaticIndexSet}), greedily, that will be used during offline
     * analysis.
     * @param arg
     *      a hot {@link HotsetSelection selection var} which contains specific values that will
     *      be utilized during the greedy hot set selection process.
     * @param <I>
     *      the {@link DBIndex} type.
     * @return
     *      a hot set (i.e., a {@link StaticIndexSet})
     */
    public static <I extends DBIndex> StaticIndexSet<I> chooseHotSetGreedy(HotsetSelection<I> arg){
        return chooseHotSetGreedy(
                arg.getCandidateSet(),
                arg.getOldHotSet(),
                arg.getRequiredIndexSet(),
                arg.getBenefitFunction(),
                arg.getMaxSize(),
                arg.isDebugOutputEnabled()
        );
    }

	static <I extends DBIndex> StaticIndexSet<I> chooseHotSet(CandidatePool.Snapshot<I> candSet,
                                                                 StaticIndexSet<I> oldHotSet,
                                                                 DynamicIndexSet<I> requiredIndexSet,
                                                                 StatisticsFunction<I> benefitFunc,
                                                                 int maxSize,
                                                                 boolean debugOutput
    ) {
		IndexBitSet emptyConfig = new IndexBitSet();
		
		int numToChoose = maxSize - requiredIndexSet.size();
		if (numToChoose <= 0) {
			return new StaticIndexSet<I>(requiredIndexSet);
		}
		else {
			MinQueue<I> topSet = new MinQueue<I>(numToChoose);

			if (debugOutput)
				System.out.println("choosing hot set:");
			for (I index : candSet) {
				if (requiredIndexSet.contains(index))
					continue;

				double penalty = oldHotSet.contains(index) ? 0 : index.creationCost();
				double score = benefitFunc.benefit(index, emptyConfig) - penalty;
				if (debugOutput)
					System.out.println(index.internalId() + " score = " + score);
				if (topSet.size() < numToChoose) {
					topSet.insertKey(index, score);
				}
				else if (topSet.minPriority() < score) {
					topSet.deleteMin();
					topSet.insertKey(index, score);
				}
			}

			java.util.ArrayList<I> list = new java.util.ArrayList<I>();
			for (I index : requiredIndexSet)
				list.add(index);
			while (topSet.size() > 0)
				list.add(topSet.deleteMin());

			return new StaticIndexSet<I>(list);
		}
	}
	
	static <I extends DBIndex> StaticIndexSet<I> chooseHotSetGreedy(CandidatePool.Snapshot<I> candSet,
                                                                       StaticIndexSet<I> oldHotSet,
                                                                       DynamicIndexSet<I> requiredIndexSet,
                                                                       StatisticsFunction<I> benefitFunc,
                                                                       int maxSize,
                                                                       boolean debugOutput
    ) {
		
		int numToChoose = maxSize - requiredIndexSet.size();
		if (numToChoose <= 0) {
			return new StaticIndexSet<I>(requiredIndexSet);
		}
		else {
			java.util.ArrayList<I> list = new java.util.ArrayList<I>();
			IndexBitSet M = new IndexBitSet();
			
			// add required indexes
			for (I index : requiredIndexSet) {
				list.add(index);
				M.set(index.internalId());
			}
			
			// add top indexes
			for (int i = 0; i < numToChoose; i++) {
				I bestIndex = null;
				double bestScore = Double.NEGATIVE_INFINITY;
				
				for (I index : candSet) {
					if (M.get(index.internalId()))
						continue;
					
					double penalty = oldHotSet.contains(index) ? 0 : index.creationCost();
					double score = benefitFunc.benefit(index, M) - penalty;
					if (score > bestScore) {
						bestIndex = index;
						bestScore = score;
					}
					
					if (debugOutput) {
						System.out.println("index " + index.internalId() + " score = " + score);
					}
				}
				if (bestIndex == null)
					break;
				else {
					list.add(bestIndex);
					M.set(bestIndex.internalId());
				}
			}

			return new StaticIndexSet<I>(list);
		}
	}


}
