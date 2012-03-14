package satuning.ibg;

import satuning.db.*;
import satuning.engine.CandidatePool.Snapshot;
import satuning.ibg.IndexBenefitGraph.IBGChild;
import satuning.ibg.IndexBenefitGraph.IBGNode;
import satuning.util.BitSet;
import satuning.util.Queue;

import java.sql.SQLException;

public class IndexBenefitGraphConstructor {
	/* 
	 * Parameters of the construction.
	 */
	protected final DBConnection conn;
	protected final String sql;
	protected final Snapshot candidateSet;
	
	/*
	 * The primary information stored by the graph
	 * 
	 * Every node in the graph is a descendant of rootNode. We also keep the
	 * cost of the workload under the empty configuration, stored in emptyCost.
	 */
	private final IBGNode rootNode;
	private double emptyCost;
	
	/* The queue of pending nodes to expand */
	Queue<IBGNode> queue = new Queue<IBGNode>();
	
	/* A monitor for waiting on a node expansion */
	private final Object nodeExpansionMonitor = new Object();

	/* An object that allows for covering node searches */
	private final IBGCoveringNodeFinder coveringNodeFinder = new IBGCoveringNodeFinder();
	
	/* true if the index is used somewhere in the graph */
	private final BitSet isUsed = new BitSet();
	
	/* Counter for assigning unique node IDs */
	private int nodeCount = 0;

	/* Temporary bit sets allocated once to allow reuse... only used in BuildNode */ 
	private final BitSet usedBitSet = new BitSet();
	private final BitSet childBitSet = new BitSet();
	
	/*
	 * Creates an IBG which is in a state ready for building.
	 * Specifically, the rootNode is physically constructed, but it is not
	 * expanded, so its cost and used set have not been determined.
	 * 
	 * In the initial state, the cost of the workload under the empty configuration
	 * is set, and may be accessed through emptyCost()
	 * 
	 * Nodes are built by calling buildNode() until it returns false.
	 */
	public IndexBenefitGraphConstructor(DBConnection conn0, String sql0, Snapshot candidateSet0) 
	throws SQLException {
		conn = conn0;
		sql = sql0;
		candidateSet = candidateSet0;

		// set up the root node, and initialize the queue
		BitSet rootConfig = candidateSet.bitSet();
		rootNode = new IBGNode(rootConfig, nodeCount++);
		
		emptyCost = conn.whatifOptimize(sql, new BitSet(), new BitSet());
		
		// initialize the queue
		queue.add(rootNode);
	}
	
	public final double emptyCost() {
		return emptyCost;
	}
	
	public final IBGNode rootNode() {
		return rootNode;
	}
	
	public final int nodeCount() {
		return nodeCount;
	}

	public final boolean isUsed(int i) {
		return isUsed.get(i);
	}
	
	public final Snapshot candidateSet() {
		return candidateSet;
	}

	/*
	 * Wait for a specific node to be expanded
	 */
	public final void waitUntilExpanded(IBGNode node) {
		synchronized (nodeExpansionMonitor) {
			while (!node.isExpanded())
				try {
					nodeExpansionMonitor.wait();
				} catch (InterruptedException e) {
					
				}
		}
	}
	
	/*
	 * Expands one node of the IBG. Returns true if a node was expanded, or false if there are no unexpanded nodes.
	 * This function is not safe to be called from more than one thread.
	 */
	public boolean buildNode() throws SQLException {
		IBGNode newNode, coveringNode;
		double totalCost;

		if (queue.isEmpty())
			return false;
		
		newNode = queue.remove();
		
		// get cost and used set (stored into usedBitSet)
		usedBitSet.clear();
		coveringNode = coveringNodeFinder.find(rootNode, newNode.config);
		if (coveringNode != null) {
			totalCost = coveringNode.cost();
			coveringNode.addUsedIndexes(usedBitSet);
		}
		else {
			totalCost = conn.whatifOptimize(sql, newNode.config, usedBitSet);
		}
		
		// create the child list
		// if any IBGNode did not exist yet, add it to the queue
		// We make sure to keep the child list in the same order as the nodeQueue, so that
		// analysis and construction can move in lock step. This is done by keeping both
		// in order of construction.
		IBGChild firstChild = null;
		IBGChild lastChild = null;
		childBitSet.set(newNode.config);
		for (int u = usedBitSet.nextSetBit(0); u >= 0; u = usedBitSet.nextSetBit(u+1)) {
			childBitSet.clear(u);
			IBGNode childNode = find(queue, childBitSet);
			if (childNode == null) {
				isUsed.set(u);
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
		
		// Expand the node and notify waiting threads
		synchronized (nodeExpansionMonitor) {
			newNode.expand(totalCost, firstChild);
			nodeExpansionMonitor.notifyAll();
		}
		
		return !queue.isEmpty();
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

	public void setEmptyCost(double cost) {
		emptyCost = cost;
	}

	public IndexBenefitGraph getIBG() {
		return new IndexBenefitGraph(rootNode, emptyCost, isUsed);
	}
}


