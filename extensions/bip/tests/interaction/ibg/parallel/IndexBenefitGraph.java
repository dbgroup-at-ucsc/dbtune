package interaction.ibg.parallel;

import interaction.db.*;
import interaction.util.BitSet;
//import interaction.util.Debug;
import interaction.util.Queue;
import interaction.workload.*;

import java.sql.SQLException;

public class IndexBenefitGraph {
	/* 
	 * Parameters of the construction.
	 * 
	 * The "workload" may very well consist of a single query
	 */
	protected final DBConnection conn;
	protected final SQLWorkload xacts;
	protected final DB2IndexSet candidateSet;
	
	/*
	 * The primary information stored by the graph
	 * 
	 * Every node in the graph is a descendant of rootNode. We also keep the
	 * cost of the workload under the empty configuration, stored in emptyCost.
	 */
	private final IBGNode rootNode;
	private double emptyCost;
	
	/* The current state of the graph construction, and various synchronization mechanisms */
//	private enum State { EMPTY, PREP, BUILDING, READY };
//	private State state;
//	private Lock stateMutex;
//	private Condition buildingCondition;
//	private Condition readyCondition;
	
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
	public IndexBenefitGraph(DBConnection conn0, SQLWorkload xacts0, DB2IndexSet candidateSet0) 
	throws SQLException {
		conn = conn0;
		xacts = xacts0;
		candidateSet = candidateSet0;
//		stateMutex = new ReentrantLock();
//		buildingCondition = stateMutex.newCondition();
//		readyCondition = stateMutex.newCondition();
//		
//		stateMutex.lock();
//		state = State.EMPTY;
//		stateMutex.unlock();

		// set up the root node, and initialize the queue
		BitSet rootConfig = candidateSet.bitSet();
		rootNode = new IBGNode(rootConfig, nodeCount++);
		
		emptyCost = conn.whatifOptimize(xacts, new BitSet(), new BitSet());
		
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

	/*
	 * Wait for a specific node to be expanded
	 */
	public final void waitUntilExpanded(IBGNode node) {
//		long start = System.currentTimeMillis();
		synchronized (nodeExpansionMonitor) {
			while (!node.isExpanded())
				try {
					nodeExpansionMonitor.wait();
				} catch (InterruptedException e) {
					
				}
		}
//		long end = System.currentTimeMillis();
//		System.out.println("waitUntilExpanded slept for "+((end-start)/1000.0)+" seconds");
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
		coveringNode = coveringNodeFinder.find(this, newNode.config);
		if (coveringNode != null) {
			totalCost = coveringNode.cost();
			coveringNode.addUsedIndexes(usedBitSet);
		}
		else {
			totalCost = conn.whatifOptimize(xacts, newNode.config, usedBitSet);
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
//			Debug.println("built node "+newNode.id+" of "+xacts.get(0).id);
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
	
	/*
	 * A node of the IBG
	 */
	public class IBGNode {
		/* Configuration that this node is about */
		public final BitSet config;
		
		/* id for the node that is unique within the enclosing IBG */
		public final int id;
		
		/* 
		 * cost with the given configuration 
		 * don't access until isExpanded() returns true
		 * 
		 * internally, this is used to determine if the node 
		 * is expanded... it is set to -1.0 until expanded
		 */
		private volatile double cost;

		/*
		 * Linked list of children
		 * don't access until isExpanded() returns true
		 */
		private volatile IBGChild firstChild;
		
		IBGNode(BitSet config0, int id0) {
			config = config0;
			id = id0;
			cost = -1.0;
			firstChild = null;
		}
		
		/*
		 * Check if it has children/cost yet
		 */
		protected final boolean isExpanded() { return cost >= 0; }
		
		/*
		 * Set the cost and list of children (one for each used index).
		 */
		protected final void expand(double cost0, IBGChild firstChild0) {
			assert(!isExpanded());
			assert(cost0 >= 0);
			
			// volatile assignments must be ordered with "state" assigned last
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
	
	protected class IBGChild {
		final int usedIndex; // the internalID of the used index on this edge
		final IBGNode node; // the actual child node
		IBGChild next = null;
		
		// next pointer is initially null
		private IBGChild(IBGNode node0, int usedIndex0) {
			node = node0;
			usedIndex = usedIndex0;
		}
	}

	public void setEmptyCost(double cost) {
		emptyCost = cost;
	}
}


