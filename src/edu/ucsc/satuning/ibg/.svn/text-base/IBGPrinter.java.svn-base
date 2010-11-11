package edu.ucsc.satuning.ibg;

import edu.ucsc.satuning.ibg.IndexBenefitGraph.IBGChild;
import edu.ucsc.satuning.ibg.IndexBenefitGraph.IBGNode;
import edu.ucsc.satuning.util.BitSet;

public class IBGPrinter {
	private final BitSet visited = new BitSet();
	private final IBGNodeQueue pending = new IBGNodeQueue();
	
	public void print(IndexBenefitGraph ibg) {
		visited.clear();
		pending.reset();
		
		pending.addNode(ibg.rootNode());
		while (pending.hasNext()) {
			IBGNode node = pending.next();

			if (visited.get(node.id)) 
				continue;
			visited.set(node.id);
			
			// we don't print unexpanded nodes... 
			// we do print the used set of expanded nodes
			if (node.isExpanded()) {
				printExpanded(ibg, node);
				pending.addChildren(node.firstChild());
			}
		}
	}
	
	private void printExpanded(IndexBenefitGraph ibg, IBGNode node) {
		boolean first;
		System.out.print("NODE:\t{");
		first = true;
		for (int i = node.config.nextSetBit(0); i >= 0; i = node.config.nextSetBit(i+1)) {
			if (ibg.isUsed(i)) {
				if (!first) System.out.print(", ");
				System.out.print(i);
				first = false;
			}
		}
		System.out.println("}");
		System.out.print("\tused {");
		first = true;
		for (IBGChild c = node.firstChild(); c != null; c = c.next) {
			if (!first) {
				System.out.print(", ");
			}
		    System.out.print(c.usedIndex);
		    first = false;
		}
		System.out.println("}");
		System.out.println("\tcost " + node.cost());
		System.out.println();
	}
}
