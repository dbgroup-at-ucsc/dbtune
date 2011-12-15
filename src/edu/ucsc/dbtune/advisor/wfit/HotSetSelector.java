package edu.ucsc.dbtune.advisor.wfit;

import edu.ucsc.dbtune.advisor.interactions.IndexStatisticsFunction;
import edu.ucsc.dbtune.metadata.Configuration;
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
                M.add(candSet.getOrdinalPosition(index));
            }
            
            // add top indexes
            for (int i = 0; i < numToChoose; i++) {
                Index bestIndex = null;
                double bestScore = Double.NEGATIVE_INFINITY;
                
                for (Index index : candSet) {
                    if (M.contains(candSet.getOrdinalPosition(index)))
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
                    M.add(candSet.getOrdinalPosition(bestIndex));
                }
            }

            return new Configuration(list);
        }
    }
}
