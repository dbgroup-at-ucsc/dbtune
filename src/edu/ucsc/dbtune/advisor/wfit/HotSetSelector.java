package edu.ucsc.dbtune.advisor.wfit;

import java.util.HashSet;
import java.util.Set;

import edu.ucsc.dbtune.advisor.interactions.IndexStatisticsFunction;
import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.util.IndexBitSet;
import edu.ucsc.dbtune.util.MinQueue;

public class HotSetSelector
{
    private HotSetSelector()
    {}

    /**
     * Choose a hot set (i.e., a {@link Configuration}) that will be used for reorganizing 
     * candidates part of a {@code snapshot}.
     *
     * @return
     *      a hot set (i.e., a {@link Configuration}) 
     */
    public static Set<Index> choose(
            Set<Index> candSet,
            Set<Index> oldHotSet,
            Set<Index> requiredIndexSet,
            IndexStatisticsFunction benefitFunc,
            int maxSize,
            boolean debugOutput
    ) {
        IndexBitSet<Index> emptyConfig = new IndexBitSet<Index>();
        
        int numToChoose = maxSize - requiredIndexSet.size();
        if (numToChoose <= 0) {
            return new HashSet<Index>(requiredIndexSet);
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

            return new HashSet<Index>(list);
        }
    }
    
    /**
     * choose a hot set (i.e., a {@link Configuration}), greedily, that will be used during offline
     * analysis.
     *
     * @return
     *      a hot set (i.e., a {@link Configuration})
     */
    public static Set<Index> chooseGreedy(
            Set<Index> candSet,
            Set<Index> oldHotSet,
            Set<Index> requiredIndexSet,
            IndexStatisticsFunction benefitFunc,
            int maxSize,
            boolean debugOutput
    ) {
        
        int numToChoose = maxSize - requiredIndexSet.size();
        if (numToChoose <= 0) {
            return new HashSet<Index>(requiredIndexSet);
        }
        else {
            java.util.ArrayList<Index> list = new java.util.ArrayList<Index>();
            IndexBitSet<Index> m = new IndexBitSet<Index>();
            
            // add required indexes
            for (Index index : requiredIndexSet) {
                list.add(index);
                m.add(index);
            }
            
            // add top indexes
            for (int i = 0; i < numToChoose; i++) {
                Index bestIndex = null;
                double bestScore = Double.NEGATIVE_INFINITY;
                
                for (Index index : candSet) {
                    if (m.contains(index))
                        continue;
                    
                    double penalty = oldHotSet.contains(index) ? 0 : index.getCreationCost();
                    double score = benefitFunc.benefit(index, m) - penalty;
                    if (score > bestScore) {
                        bestIndex = index;
                        bestScore = score;
                    }
                }
                if (bestIndex == null)
                    break;
                else {
                    list.add(bestIndex);
                    m.add(bestIndex);
                }
            }

            return new HashSet<Index>(list);
        }
    }
}
