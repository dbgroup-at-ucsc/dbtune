package interaction.ibg.serial;

import interaction.ibg.serial.SerialIndexBenefitGraph.IBGNode;
import interaction.schedule.IBGScheduleInfo;
import interaction.util.BitSet;

public class SerialIBGCoveringNodeFinder implements IBGScheduleInfo.Searcher<SerialIndexBenefitGraph> {
	private final BitSet visited = new BitSet();
	private final SerialIBGNodeStack pending = new SerialIBGNodeStack();
	
	public void find(SerialIndexBenefitGraph ibg, BitSet[] configs, int configCount, IBGNode[] outNodes) {
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

	public final IBGNode find(SerialIndexBenefitGraph ibg, BitSet config) {
		return find(ibg.rootNode(), config);
	}
	
	public final IBGNode find(IBGNode rootNode, BitSet config) {
		visited.clear();
		pending.reset();
		
		pending.addNode(rootNode);
		while (pending.hasNext()) {
			IBGNode node = pending.next();
			
			if (visited.get(node.id)) 
				continue;
			visited.set(node.id);
			
			// prune non-supersets and unexpanded nodes
			if (!config.subsetOf(node.config) || !node.isExpanded())
				continue;

			// return if we have found covering node
			if (node.usedSetIsSubsetOf(config))
				return node;

			// this node has children that might be covering nodes... continue on
			pending.addChildren(node.firstChild());
		}
		
		return null;
	}
	
	public final double findCost(SerialIndexBenefitGraph ibg, BitSet config) {
		if (config.isEmpty())
			return ibg.emptyCost();
		else
			return find(ibg, config).cost();
	}
	
	public final double findCost(SerialIndexBenefitGraph[] ibgs, BitSet config) {
		double cost = 0;
		for (SerialIndexBenefitGraph ibg : ibgs)
			cost += findCost(ibg, config);
		return cost;
	}
}
