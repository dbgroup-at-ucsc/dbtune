package edu.ucsc.dbtune.advisor;

import edu.ucsc.dbtune.core.DBIndex;
import edu.ucsc.dbtune.ibg.CandidatePool.Snapshot;
import edu.ucsc.dbtune.util.IndexBitSet;
import edu.ucsc.dbtune.util.MinQueue;

/**
 * ...
 *
 * @author hsanchez@cs.ucsc.edu (Huascar A. Sanchez)
 */
public class KarlsHotsetSelector {
	public static StaticIndexSet
	chooseHotSet(Snapshot candSet,
	             StaticIndexSet oldHotSet,
	             DynamicIndexSet requiredIndexSet,
	             BenefitFunction benefitFunc,
	             int maxSize,
	             boolean debugOutput) {
		IndexBitSet emptyConfig = new IndexBitSet();

		int numToChoose = maxSize - requiredIndexSet.size();
		if (numToChoose <= 0) {
			return new StaticIndexSet(requiredIndexSet);
		}
		else {
			MinQueue<DBIndex> topSet = new MinQueue<DBIndex>(numToChoose);

			if (debugOutput)
				System.out.println("choosing hot set:");
			for (DBIndex index : candSet) {
				if (requiredIndexSet.contains(index))
					continue;

				double penalty = oldHotSet.contains(index) ? 0 : index.creationCost();
				double score = benefitFunc.apply(index, emptyConfig) - penalty;
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

			java.util.ArrayList<DBIndex> list = new java.util.ArrayList<DBIndex>();
			for (DBIndex index : requiredIndexSet)
				list.add(index);
			while (topSet.size() > 0)
				list.add(topSet.deleteMin());

			return new StaticIndexSet(list);
		}
	}

	public static StaticIndexSet
	chooseHotSetGreedy(Snapshot candSet,
	             StaticIndexSet oldHotSet,
	             DynamicIndexSet requiredIndexSet,
	             BenefitFunction benefitFunc,
	             int maxSize,
	             boolean debugOutput) {

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
					double score = benefitFunc.apply(index, M) - penalty;
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

			return new StaticIndexSet(list);
		}
	}
}
