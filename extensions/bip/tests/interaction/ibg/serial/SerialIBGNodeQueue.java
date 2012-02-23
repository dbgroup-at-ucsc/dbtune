package interaction.ibg.serial;

import interaction.ibg.serial.SerialIndexBenefitGraph.IBGChild;
import interaction.ibg.serial.SerialIndexBenefitGraph.IBGNode;

public class SerialIBGNodeQueue {
	private interaction.util.Queue<Object> queue = new interaction.util.Queue<Object>();
	
	// add child and all siblings (following next pointers) to the queue
	final void addChildren(IBGChild ch) {
		if (ch != null) queue.add(ch);
	}
	// add a single node to the queue
	final void addNode(IBGNode node) {
		queue.add(node);
	}
	// test if the queue has something remaining
	final boolean hasNext() {
		return !queue.isEmpty();
	}
	// remove all queued nodes
	final void reset() {
		queue.clear();
	}
	// get the next node, or return null if none
	final IBGNode next() {
		if (queue.isEmpty())
			return null;
		
		Object obj = queue.peek();
		if (obj instanceof IBGChild) {
			IBGChild child = (IBGChild) obj	;
			if (child.next != null)
				queue.replace(0, child.next);
			else 
				queue.remove();
			return child.node;
		}
		else {
			queue.remove();
			return (IBGNode) obj;
		}
	}
	
	public IBGNode peek() {
		if (queue.isEmpty())
			return null;
		
		Object obj = queue.peek();
		if (obj instanceof IBGChild) {
			IBGChild child = (IBGChild) obj	;
			return child.node;
		}
		else {
			return (IBGNode) obj;
		}
	}
}
