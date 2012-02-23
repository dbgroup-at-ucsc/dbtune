package interaction.ibg.serial;

import interaction.ibg.serial.SerialIndexBenefitGraph.IBGNode;
import interaction.util.BitSet;

public class SerialIBGMonotonicEnforcer {
	private final BitSet visited = new BitSet();
	private final SerialIBGNodeQueue pending = new SerialIBGNodeQueue();	
	private SubSearch sub = new SubSearch();
	
	public void fix(SerialIndexBenefitGraph ibg) {
		visited.clear();
		pending.reset();
		
		pending.addNode(ibg.rootNode());
		while (pending.hasNext()) {
			IBGNode node = pending.next();
			
			if (visited.get(node.id)) 
				continue;
			visited.set(node.id);
			
			sub.fixSubsets(ibg, node.config, node.cost());
			
			if (node.firstChild() == null) {
				if (node.cost() > ibg.emptyCost()) {
					ibg.setEmptyCost(node.cost());
				}
			}
			else
				pending.addChildren(node.firstChild());
		}	
	}
	
	private static class SubSearch {
		private final BitSet visited = new BitSet();
		private final SerialIBGNodeStack pending = new SerialIBGNodeStack();
		
		private void fixSubsets(SerialIndexBenefitGraph ibg, BitSet config, double cost) {
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
						node.setCost(cost);
					}
				}
				else 
					pending.addChildren(node.firstChild());
			}		
		}
	}
}
