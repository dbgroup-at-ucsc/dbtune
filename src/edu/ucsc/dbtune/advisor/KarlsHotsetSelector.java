package edu.ucsc.dbtune.advisor;

import edu.ucsc.dbtune.ibg.CandidatePool.Snapshot;
import edu.ucsc.dbtune.metadata.Index;
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
			MinQueue<Index> topSet = new MinQueue<Index>(numToChoose);

			if (debugOutput)
				System.out.println("choosing hot set:");
			for (Index index : candSet) {
				if (requiredIndexSet.contains(index))
					continue;

				double penalty = oldHotSet.contains(index) ? 0 : index.getCreationCost();
				double score = benefitFunc.apply(index, emptyConfig) - penalty;
				if (debugOutput)
					System.out.println(index.getId() + " score = " + score);
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
			java.util.ArrayList<Index> list = new java.util.ArrayList<Index>();
			IndexBitSet M = new IndexBitSet();

			// add required indexes
			for (Index index : requiredIndexSet) {
				list.add(index);
				M.set(index.getId());
			}

			// add top indexes
			for (int i = 0; i < numToChoose; i++) {
				Index bestIndex = null;
				double bestScore = Double.NEGATIVE_INFINITY;

				for (Index index : candSet) {
					if (M.get(index.getId()))
						continue;

					double penalty = oldHotSet.contains(index) ? 0 : index.getCreationCost();
					double score = benefitFunc.apply(index, M) - penalty;
					if (score > bestScore) {
						bestIndex = index;
						bestScore = score;
					}

					if (debugOutput) {
						System.out.println("index " + index.getId() + " score = " + score);
					}
				}
				if (bestIndex == null)
					break;
				else {
					list.add(bestIndex);
					M.set(bestIndex.getId());
				}
			}

			return new StaticIndexSet(list);
		}
	}
}
