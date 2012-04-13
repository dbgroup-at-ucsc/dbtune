package edu.ucsc.dbtune.advisor.wfit;

import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.advisor.wfit.CandidatePool.Snapshot;

public class HotSetSelector {
	public static StaticIndexSet chooseHotSet(Snapshot candSet, 
			                                  StaticIndexSet oldHotSet, 
			                                  DynamicIndexSet requiredIndexSet,
			                                  BenefitFunction benefitFunc, 
			                                  int maxSize) {
		int numToChoose = maxSize - requiredIndexSet.size();
		if (numToChoose <= 0) {
			return new StaticIndexSet(requiredIndexSet.toArray());
		}
		else {
            MinQueue<Index> topSet = new MinQueue<Index>(numToChoose);

            for (Index index : candSet) {
				if (requiredIndexSet.contains(index))
					continue;

                double penalty = oldHotSet.contains(index) ? 0 : index.getCreationCost();
				double score = benefitFunc.benefit(index) - penalty;
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

            Index[] hotArray = new Index[list.size()];
			int i = 0;
            for (Index index : list) hotArray[i++] = index;
			return new StaticIndexSet(hotArray);
		}
	}
}
