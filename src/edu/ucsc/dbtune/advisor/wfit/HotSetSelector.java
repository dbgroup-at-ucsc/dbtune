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

package edu.ucsc.dbtune.advisor.wfit;

import edu.ucsc.dbtune.advisor.interactions.IndexStatisticsFunction;
import edu.ucsc.dbtune.metadata.Configuration;
import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.util.IndexBitSet;
import edu.ucsc.dbtune.util.MinQueue;

public class HotSetSelector
{
    private HotSetSelector(){}

    /**
     * Choose a hot set (i.e., a {@link Configuration}) that will be used for reorganizing 
     * candidates part of a {@code snapshot}.
     *
     * @return
     *      a hot set (i.e., a {@link Configuration}) 
     */
    public static Configuration choose(
            Configuration candSet,
            Configuration oldHotSet,
            Configuration requiredIndexSet,
            IndexStatisticsFunction benefitFunc,
            int maxSize,
            boolean debugOutput
    ) {
        IndexBitSet emptyConfig = new IndexBitSet();
        
        int numToChoose = maxSize - requiredIndexSet.size();
        if (numToChoose <= 0) {
            return new Configuration(requiredIndexSet);
        }
        else {
            MinQueue<Index> topSet = new MinQueue<Index>(numToChoose);

            for (Index index : candSet) {
                if (requiredIndexSet.contains(index))
                    continue;

                double penalty = oldHotSet.contains(index) ? 0 : index.getCreationCost();
                double score = benefitFunc.benefit(index, emptyConfig) - penalty;
                if (topSet.size() < numToChoose) {
                    topSet.insertKey(index, score);
                }
                else if (topSet.minPriority() < score) {
                    topSet.deleteMin();
                    topSet.insertKey(index, score);
                }
            }

            java.util.ArrayList<Index> list = new java.util.ArrayList<Index>();
            for (Index index : requiredIndexSet)
                list.add(index);
            while (topSet.size() > 0)
                list.add(topSet.deleteMin());

            return new Configuration(list);
        }
    }
    
    /**
     * choose a hot set (i.e., a {@link Configuration}), greedily, that will be used during offline
     * analysis.
     *
     * @return
     *      a hot set (i.e., a {@link Configuration})
     */
    public static Configuration chooseGreedy(
            Configuration candSet,
            Configuration oldHotSet,
            Configuration requiredIndexSet,
            IndexStatisticsFunction benefitFunc,
            int maxSize,
            boolean debugOutput
    ) {
        
        int numToChoose = maxSize - requiredIndexSet.size();
        if (numToChoose <= 0) {
            return new Configuration(requiredIndexSet);
        }
        else {
            java.util.ArrayList<Index> list = new java.util.ArrayList<Index>();
            IndexBitSet M = new IndexBitSet();
            
            // add required indexes
            for (Index index : requiredIndexSet) {
                list.add(index);
                M.set(candSet.getOrdinalPosition(index));
            }
            
            // add top indexes
            for (int i = 0; i < numToChoose; i++) {
                Index bestIndex = null;
                double bestScore = Double.NEGATIVE_INFINITY;
                
                for (Index index : candSet) {
                    if (M.get(candSet.getOrdinalPosition(index)))
                        continue;
                    
                    double penalty = oldHotSet.contains(index) ? 0 : index.getCreationCost();
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
                    M.set(candSet.getOrdinalPosition(bestIndex));
                }
            }

            return new Configuration(list);
        }
    }
}
