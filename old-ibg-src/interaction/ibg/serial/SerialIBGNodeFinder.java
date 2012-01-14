package interaction.ibg.serial;

import interaction.ibg.serial.SerialIndexBenefitGraph.IBGNode;
import interaction.util.BitSet;

/*
 * IBGNodeFinder -- does a search for a particular node in the graph
 */
class SerialIBGNodeFinder {
	private final BitSet visited = new BitSet();
	private final SerialIBGNodeStack pending = new SerialIBGNodeStack();
	
	public IBGNode find(IBGNode rootNode, BitSet config) {
		visited.clear();
		pending.reset();
		
		pending.addNode(rootNode);
		while (pending.hasNext()) {
			IBGNode node = pending.next();
			
			if (visited.get(node.id)) 
				continue;
			visited.set(node.id);
			
			// we can prune the search if the node does not contain all of config
			if (!config.subsetOf(node.config)) 
				continue;
			
			// we can stop the search if the node matches exactly
			if (node.config.equals(config)) 
				return node;
			
			pending.addChildren(node.firstChild());
		}	
		
		return null;
	}
}
