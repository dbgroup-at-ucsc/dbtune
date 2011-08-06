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
     * @param 
     *      the {@link DBIndex} type.
     * @return
     *      a hot set (i.e., a {@link StaticIndexSet}) 
     */
    public static StaticIndexSet chooseHotSet(HotsetSelection arg){
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
     * @param 
     *      the {@link DBIndex} type.
     * @return
     *      a hot set (i.e., a {@link StaticIndexSet})
     */
    public static StaticIndexSet chooseHotSetGreedy(HotsetSelection arg){
        return chooseHotSetGreedy(
                arg.getCandidateSet(),
                arg.getOldHotSet(),
                arg.getRequiredIndexSet(),
                arg.getBenefitFunction(),
                arg.getMaxSize(),
                arg.isDebugOutputEnabled()
        );
    }

    static StaticIndexSet chooseHotSet(CandidatePool.Snapshot candSet,
                                                                 StaticIndexSet oldHotSet,
                                                                 DynamicIndexSet requiredIndexSet,
                                                                 StatisticsFunction benefitFunc,
                                                                 int maxSize,
                                                                 boolean debugOutput
    ) {
        IndexBitSet emptyConfig = new IndexBitSet();
        
        int numToChoose = maxSize - requiredIndexSet.size();
        if (numToChoose <= 0) {
            return new StaticIndexSet(requiredIndexSet);
        }
        else {
            MinQueue<DBIndex> topSet = new MinQueue<DBIndex>(numToChoose);

            for (DBIndex index : candSet) {
                if (requiredIndexSet.contains(index))
                    continue;

                double penalty = oldHotSet.contains(index) ? 0 : index.creationCost();
                double score = benefitFunc.benefit(index, emptyConfig) - penalty;
                if (topSet.size() < numToChoose) {
                    topSet.insertKey(index, score);
                }
                else if (topSet.minPriority() < score) {
                    topSet.deleteMin();
                    topSet.insertKey(index, score);
                }
            }

            java.util.ArrayList<DBIndex> list = new java.util.ArrayList<DBIndex>();
            for (DBIndex index : requiredIndexSet)
                list.add(index);
            while (topSet.size() > 0)
                list.add(topSet.deleteMin());

            return new StaticIndexSet(list);
        }
    }
    
    static StaticIndexSet chooseHotSetGreedy(CandidatePool.Snapshot candSet,
                                                                       StaticIndexSet oldHotSet,
                                                                       DynamicIndexSet requiredIndexSet,
                                                                       StatisticsFunction benefitFunc,
                                                                       int maxSize,
                                                                       boolean debugOutput
    ) {
        
        int numToChoose = maxSize - requiredIndexSet.size();
        if (numToChoose <= 0) {
            return new StaticIndexSet(requiredIndexSet);
        }
        else {
            java.util.ArrayList<DBIndex> list = new java.util.ArrayList<DBIndex>();
            IndexBitSet M = new IndexBitSet();
            
            // add required indexes
            for (DBIndex index : requiredIndexSet) {
                list.add(index);
                M.set(index.internalId());
            }
            
            // add top indexes
            for (int i = 0; i < numToChoose; i++) {
                DBIndex bestIndex = null;
                double bestScore = Double.NEGATIVE_INFINITY;
                
                for (DBIndex index : candSet) {
                    if (M.get(index.internalId()))
                        continue;
                    
                    double penalty = oldHotSet.contains(index) ? 0 : index.creationCost();
                    double score = benefitFunc.benefit(index, M) - penalty;
                    if (score > bestScore) {
                        bestIndex = index;
                        bestScore = score;
                    }
                }
                if (bestIndex == null)
                    break;
                else {
                    list.add(bestIndex);
                    M.set(bestIndex.internalId());
                }
            }

            return new StaticIndexSet(list);
        }
    }


}
