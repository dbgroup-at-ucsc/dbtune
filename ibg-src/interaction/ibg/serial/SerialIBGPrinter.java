package interaction.ibg.serial;

import interaction.ibg.serial.SerialIndexBenefitGraph.IBGChild;
import interaction.ibg.serial.SerialIndexBenefitGraph.IBGNode;
import interaction.util.BitSet;

public class SerialIBGPrinter {
	private final BitSet visited = new BitSet();
	private final SerialIBGNodeQueue pending = new SerialIBGNodeQueue();
	
	public void print(SerialIndexBenefitGraph ibg) {
		visited.clear();
		pending.reset();
		
		pending.addNode(ibg.rootNode());
		while (pending.hasNext()) {
			IBGNode node = pending.next();

			if (visited.get(node.id)) 
				continue;
			visited.set(node.id);
			
			printNode(ibg, node);
			pending.addChildren(node.firstChild());
		}
	}
	
	private final void printNode(SerialIndexBenefitGraph ibg, IBGNode node) {
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
