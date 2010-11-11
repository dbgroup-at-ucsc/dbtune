package edu.ucsc.satuning.ibg;

import edu.ucsc.satuning.ibg.IndexBenefitGraph.IBGNode;
import edu.ucsc.satuning.util.BitSet;

public class IBGBestBenefitFinder {
	private final BitSet visited = new BitSet();
	private final BitSet bitset_Ya = new BitSet();
	private final IBGNodeQueue pending = new IBGNodeQueue();
	private final IBGCoveringNodeFinder finder = new IBGCoveringNodeFinder();
	
	public double bestBenefit(IndexBenefitGraph ibg, int indexId, BitSet M) {
		visited.clear();
		pending.reset();
		
		double bestValue = 0;
		
		pending.addNode(ibg.rootNode());
		while (pending.hasNext()) {
			IBGNode Y = pending.next();

			if (visited.get(Y.id)) 
				continue;
			visited.set(Y.id);

			if (!Y.config.get(indexId) && M.subsetOf(Y.config)) {
				bitset_Ya.set(Y.config);
				bitset_Ya.set(indexId);
				IBGNode Ya = finder.findFast(ibg.rootNode(), bitset_Ya, null);
				double value = Y.cost() - Ya.cost();
				bestValue = Math.max(value, bestValue);
			}
			pending.addChildren(Y.firstChild());
		}
		
		return bestValue;
	}
}
