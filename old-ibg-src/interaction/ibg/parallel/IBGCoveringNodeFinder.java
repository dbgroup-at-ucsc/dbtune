package interaction.ibg.parallel;

import interaction.ibg.parallel.IndexBenefitGraph.IBGNode;
import interaction.util.BitSet;

public class IBGCoveringNodeFinder {
	private final BitSet visited = new BitSet();
	private final IBGNodeStack pending = new IBGNodeStack();
	
	public void find(IndexBenefitGraph ibg, BitSet[] configs, int configCount, IBGNode[] outNodes) {
		for (int i = 0; i < configCount; i++) {
			assert(configs[i] != null);
			outNodes[i] = null;
		}
		
		visited.clear();
		pending.reset();
		
		pending.addNode(ibg.rootNode());
		while (pending.hasNext()) {
			IBGNode node = pending.next();
			
			if (visited.get(node.id)) 
				continue;
			visited.set(node.id);
			
			if (!node.isExpanded())
				continue;
			
			boolean missingCoveringNode = false; 
			boolean supersetOfMissing = false;
			for (int i = 0; i < configCount; i++) {
				if (outNodes[i] == null) {
					boolean subset = configs[i].subsetOf(node.config);
					boolean containsUsed = node.usedSetIsSubsetOf(configs[i]);
					if (subset && containsUsed) {
						outNodes[i] = node;
					}
					else {
						missingCoveringNode = true;
						supersetOfMissing = supersetOfMissing || subset;
					}
				}
			}
			
			if (!missingCoveringNode)
				return;
			if (supersetOfMissing)
				pending.addChildren(node.firstChild());
		}	
	}

	public IBGNode find(IndexBenefitGraph ibg, BitSet config) {
		visited.clear();
		pending.reset();
		
		pending.addNode(ibg.rootNode());
		while (pending.hasNext()) {
			IBGNode node = pending.next();
			
			if (visited.get(node.id)) 
				continue;
			visited.set(node.id);
			
			// skip unexpanded nodes
			if (!node.isExpanded())
				continue;
			
			// prune non-supersets
			if (!config.subsetOf(node.config))
				continue;
			
			// return if we have found covering node
			if (node.usedSetIsSubsetOf(config))
				return node;
			
			// this node has children that might be covering nodes... continue on
			pending.addChildren(node.firstChild());
		}
		
		return null;
	}
	
	public final double findCost(IndexBenefitGraph ibg, BitSet config) {
		if (config.isEmpty())
			return ibg.emptyCost();
		else
			return find(ibg, config).cost();
	}
	
	public final double findCost(IndexBenefitGraph[] ibgs, BitSet config) {
		double cost = 0;
		for (IndexBenefitGraph ibg : ibgs)
			cost += findCost(ibg, config);
		return cost;
	}
}
