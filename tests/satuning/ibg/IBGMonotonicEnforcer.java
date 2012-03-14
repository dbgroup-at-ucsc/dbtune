package satuning.ibg;

import satuning.ibg.IndexBenefitGraph.IBGNode;
import satuning.util.BitSet;

public class IBGMonotonicEnforcer {
	private final BitSet visited = new BitSet();
	private final IBGNodeQueue pending = new IBGNodeQueue();	
	private SubSearch sub = new SubSearch();
	
	public void fix(IndexBenefitGraph ibg) {
		visited.clear();
		pending.reset();
		
		pending.addNode(ibg.rootNode());
		while (pending.hasNext()) {
			IBGNode node = pending.next();
			
			if (visited.get(node.id)) 
				continue;
			visited.set(node.id);
			
			sub.fixSubsets(ibg, node.config, node.cost());
			
			if (node.isExpanded()) {
				if (node.firstChild() == null) {
					if (node.cost() > ibg.emptyCost()) {
						System.out.flush();
						System.err.printf("monotonicity violation: %f%% ", 100.0*(node.cost() - ibg.emptyCost())/node.cost());
						System.err.flush();
						ibg.setEmptyCost(node.cost());
					}
				}
				else
					pending.addChildren(node.firstChild());
			}
		}	
	}
	
	private static class SubSearch {
		private final BitSet visited = new BitSet();
		private final IBGNodeStack pending = new IBGNodeStack();
		
		private void fixSubsets(IndexBenefitGraph ibg, BitSet config, double cost) {
			visited.clear();
			pending.reset();
			
			pending.addNode(ibg.rootNode());
			while (pending.hasNext()) {
				IBGNode node = pending.next();
				
				if (visited.get(node.id)) 
					continue;
				visited.set(node.id);
				
				if (node.config.subsetOf(config) && !node.config.equals(config)) {
					if (node.cost() < cost) {
						System.out.flush();
						System.err.printf("monotonicity violation: %f%% ", 100.0*(node.cost() - cost)/node.cost());
						System.err.flush();
						node.setCost(cost);
					}
				}
				else if (node.isExpanded()) 
					pending.addChildren(node.firstChild());
			}		
		}
	}
}
