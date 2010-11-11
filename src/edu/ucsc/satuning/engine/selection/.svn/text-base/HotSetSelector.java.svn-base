package edu.ucsc.satuning.engine.selection;

import edu.ucsc.satuning.db.DBIndex;
import edu.ucsc.satuning.engine.CandidatePool.Snapshot;
import edu.ucsc.satuning.util.BitSet;
import edu.ucsc.satuning.util.MinQueue;

public class HotSetSelector {
	public static <I extends DBIndex<I>> StaticIndexSet<I> 
	chooseHotSet(Snapshot<I> candSet, 
	             StaticIndexSet<I> oldHotSet, 
	             DynamicIndexSet<I> requiredIndexSet,
	             BenefitFunction<I> benefitFunc, 
	             int maxSize,
	             boolean debugOutput) {
		BitSet emptyConfig = new BitSet();
		
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
	
	public static <I extends DBIndex<I>> StaticIndexSet<I> 
	chooseHotSetGreedy(Snapshot<I> candSet, 
	             StaticIndexSet<I> oldHotSet, 
	             DynamicIndexSet<I> requiredIndexSet,
	             BenefitFunction<I> benefitFunc, 
	             int maxSize,
	             boolean debugOutput) {
		
		int numToChoose = maxSize - requiredIndexSet.size();
		if (numToChoose <= 0) {
			return new StaticIndexSet<I>(requiredIndexSet);
		}
		else {
			java.util.ArrayList<I> list = new java.util.ArrayList<I>();
			BitSet M = new BitSet();
			
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
