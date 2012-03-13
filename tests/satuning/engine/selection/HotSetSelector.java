package satuning.engine.selection;

import satuning.db.DB2Index;
import satuning.engine.CandidatePool.Snapshot;
import satuning.util.MinQueue;

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
			MinQueue<DB2Index> topSet = new MinQueue<DB2Index>(numToChoose);

			for (DB2Index index : candSet) {
				if (requiredIndexSet.contains(index))
					continue;

				double penalty = oldHotSet.contains(index) ? 0 : index.creationCost();
				double score = benefitFunc.benefit(index) - penalty;
				if (topSet.size() < numToChoose) {
					topSet.insertKey(index, score);
				}
				else if (topSet.minPriority() < score) {
					topSet.deleteMin();
					topSet.insertKey(index, score);
				}
			}

			java.util.ArrayList<DB2Index> list = new java.util.ArrayList<DB2Index>();
			for (DB2Index index : requiredIndexSet)
				list.add(index);
			while (topSet.size() > 0)
				list.add(topSet.deleteMin());

			DB2Index[] hotArray = new DB2Index[list.size()];
			int i = 0;
			for (DB2Index index : list) 
				hotArray[i++] = index;
			return new StaticIndexSet(hotArray);
		}
	}
}
