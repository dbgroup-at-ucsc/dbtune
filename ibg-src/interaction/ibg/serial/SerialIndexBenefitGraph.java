package interaction.ibg.serial;

import interaction.db.*;
import interaction.util.BitSet;
import interaction.util.Queue;
import interaction.workload.*;

import java.io.Serializable;
import java.sql.SQLException;

public class SerialIndexBenefitGraph implements Serializable {
	private static final long serialVersionUID = 1L;

	/*
	 * The primary information stored by the graph
	 * 
	 * Every node in the graph is a descendant of rootNode. We also keep the
	 * cost of the workload under the empty configuration, stored in emptyCost.
	 */
	private final IBGNode rootNode;
	private double emptyCost;

	/* true if the index is used somewhere in the graph */
	private final BitSet usedSet;

	/* getting size of the graph */
	private final int nodeCount;
	
	/*
	 * Creates an IBG 
	 */
	private SerialIndexBenefitGraph(IBGNode rootNode, double emptyCost, BitSet usedSet, int nodeCount) {
		this.rootNode = rootNode;
		this.emptyCost = emptyCost;
		this.usedSet = usedSet;
		this.nodeCount = nodeCount;
	}
	
	public final double emptyCost() {
		return emptyCost;
	}

	public void setEmptyCost(double cost) {
		emptyCost = cost;
	}
	
	public final IBGNode rootNode() {
		return rootNode;
	}
	
	public final int nodeCount() {
		return nodeCount;
	}

	public final boolean isUsed(int i) {
		return usedSet.get(i);
	}
	
	/*
	 * Does construction after everything except the nodes are initialized
	 */
	public static SerialIndexBenefitGraph build(DBConnection conn, SQLWorkload xacts, DB2IndexSet candidateSet) throws SQLException { 
		BitSet allUsedIndexes = new BitSet();
		BitSet tempUsedIndexes = new BitSet();
		BitSet childBitSet = new BitSet();
		SerialIBGCoveringNodeFinder coveringNodeFinder = new SerialIBGCoveringNodeFinder();
		
		int nodeCount = 0;
		IBGNode rootNode = new IBGNode(candidateSet.bitSet(), nodeCount++);
		Queue<IBGNode> queue = new Queue<IBGNode>();
		queue.add(rootNode);
		while (!queue.isEmpty()) {

			IBGNode newNode, coveringNode;
			double totalCost;
		
			newNode = queue.remove();
		
			// get cost and used set (stored into tempUsedIndexes)
			tempUsedIndexes.clear();
			coveringNode = coveringNodeFinder.find(rootNode, newNode.config);
			if (coveringNode != null) {
				totalCost = coveringNode.cost();
				coveringNode.addUsedIndexes(tempUsedIndexes);
			}
			else {
				totalCost = conn.whatifOptimize(xacts, newNode.config, tempUsedIndexes);
			}
		
			// create the child list
			// if any IBGNode did not exist yet, add it to the queue
			// We make sure to keep the child list in the same order as the nodeQueue, so that
			// analysis and construction can move in lock step. This is done by keeping both
			// in order of construction.
			IBGChild firstChild = null;
			IBGChild lastChild = null;
			childBitSet.set(newNode.config);
			for (int u = tempUsedIndexes.nextSetBit(0); u >= 0; u = tempUsedIndexes.nextSetBit(u+1)) {
				childBitSet.clear(u);
				IBGNode childNode = find(queue, childBitSet);
				if (childNode == null) {
					allUsedIndexes.set(u);
					childNode = new IBGNode((BitSet) childBitSet.clone(), nodeCount++);
					queue.add(childNode);
				}
				childBitSet.set(u);
				
				IBGChild child = new IBGChild(childNode, u);
				if (firstChild == null) {
					firstChild = lastChild = child;
				}
				else {
					lastChild.next = child;
					lastChild = child;
				}
			}
				
			newNode.expand(totalCost, firstChild);
		}
		
		double emptyCost = conn.whatifOptimize(xacts, new BitSet(), tempUsedIndexes);
		return new SerialIndexBenefitGraph(rootNode, emptyCost, allUsedIndexes, nodeCount);
	}
	
	/*
	 * Auxiliary method for buildNodes
	 */
	private static IBGNode find(Queue<IBGNode> queue, BitSet config) {
		for (int i = 0; i < queue.count(); i++) {
			IBGNode node = queue.fetch(i);
			if (node.config.equals(config))
				return node;
		}
		return null;
	}
	
	/*
	 * A node of the IBG
	 */
	public static class IBGNode implements Serializable {
		private static final long serialVersionUID = 1L;

		/* Configuration that this node is about */
		public final BitSet config;
		
		/* id for the node that is unique within the enclosing IBG */
		public final int id;
		
		/* 
		 * cost with the given configuration 
		 * 
		 * internally, this is used to determine if the node 
		 * is expanded... it is set to -1.0 until expanded
		 */
		private double cost;

		/*
		 * Linked list of children
		 * don't access until isExpanded() returns true
		 */
		private IBGChild firstChild;
		
		IBGNode(BitSet config0, int id0) {
			config = config0;
			id = id0;
			cost = -1.0;
			firstChild = null;
		}
		
		/*
		 * Check if it has children/cost yet
		 */
		final boolean isExpanded() { return cost >= 0; }
		
		/*
		 * Set the cost and list of children (one for each used index).
		 */
		protected final void expand(double cost0, IBGChild firstChild0) {
			assert(!isExpanded());
			assert(cost0 >= 0);
			
			cost = cost0;
			firstChild = firstChild0;
		}
		
		/*
		 * Get the cost
		 */
		public final double cost() {
			assert(isExpanded());
			return cost;
		}
		
		/*
		 * Get the head of the child list
		 */
		protected final IBGChild firstChild() {
			assert(isExpanded());
			return firstChild; 
		}

		/*
		 * Add each of the used indexes in this node to the given BitSet
		 */
		public final void addUsedIndexes(BitSet bs) {
			assert(isExpanded());
			for (IBGChild ch = firstChild; ch != null; ch = ch.next)
				bs.set(ch.usedIndex);
		}
		
		/*
		 * Remove each of the used indexes in this node from the given BitSet
		 */
		public void clearUsedIndexes(BitSet bs) {
			assert(isExpanded());
			for (IBGChild ch = firstChild; ch != null; ch = ch.next)
				bs.clear(ch.usedIndex);
		}
		
		/*
		 * return true if each of the used indexes are in the given BitSet
		 */
		public boolean usedSetIsSubsetOf(BitSet other) {
			assert(isExpanded());
			for (IBGChild ch = firstChild; ch != null; ch = ch.next)
				if (!other.get(ch.usedIndex))
					return false;
			return true;
		}
		
		/*
		 * return true if the i is in the used set
		 */
		public boolean usedSetContains(int id) {
			assert(isExpanded());
			for (IBGChild ch = firstChild; ch != null; ch = ch.next)
				if (id == ch.usedIndex)
					return true;
			return false;
		}

		public void setCost(double cost0) {
			cost = cost0;
		}
	}
	
	protected static class IBGChild implements Serializable {
		private static final long serialVersionUID = 1L;
		
		final int usedIndex; // the internalID of the used index on this edge
		final IBGNode node; // the actual child node
		IBGChild next = null;
		
		// next pointer is initially null
		private IBGChild(IBGNode node0, int usedIndex0) {
			node = node0;
			usedIndex = usedIndex0;
		}
	}

	public String usedSetToString() {
		return usedSet.toString();
	}

	public BitSet usedSet() {
		return (BitSet) usedSet.clone();
	}
}


