/*
 * ****************************************************************************
 *   Copyright 2010 University of California Santa Cruz                       *
 *                                                                            *
 *   Licensed under the Apache License, Version 2.0 (the "License");          *
 *   you may not use this file except in compliance with the License.         *
 *   You may obtain a copy of the License at                                  *
 *                                                                            *
 *       http://www.apache.org/licenses/LICENSE-2.0                           *
 *                                                                            *
 *   Unless required by applicable law or agreed to in writing, software      *
 *   distributed under the License is distributed on an "AS IS" BASIS,        *
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. *
 *   See the License for the specific language governing permissions and      *
 *   limitations under the License.                                           *
 *  ****************************************************************************
 */

package edu.ucsc.dbtune.ibg;

import edu.ucsc.dbtune.core.DBIndex;
import edu.ucsc.dbtune.core.DatabaseConnection;
import edu.ucsc.dbtune.ibg.CandidatePool.Snapshot;
import edu.ucsc.dbtune.ibg.IndexBenefitGraph.IBGChild;
import edu.ucsc.dbtune.ibg.IndexBenefitGraph.IBGNode;
import edu.ucsc.dbtune.util.DefaultBitSet;
import edu.ucsc.dbtune.util.DefaultQueue;
import edu.ucsc.dbtune.util.Objects;

import java.sql.SQLException;

public class IndexBenefitGraphConstructor<I extends DBIndex<I>> {
	/* 
	 * Parameters of the construction.
	 */
	protected final DatabaseConnection<I> conn;
	protected final String sql;
	protected final Snapshot<I> candidateSet;
	
	/*
	 * The primary information stored by the graph
	 * 
	 * Every node in the graph is a descendant of rootNode. We also keep the
	 * cost of the workload under the empty configuration, stored in emptyCost.
	 */
	private final IBGNode rootNode;
	private double emptyCost;
	
	/* The queue of pending nodes to expand */
	DefaultQueue<IBGNode> queue = new DefaultQueue<IBGNode>();
	
	/* A monitor for waiting on a node expansion */
	private final Object nodeExpansionMonitor = new Object();

	/* An object that allows for covering node searches */
	private final IBGCoveringNodeFinder coveringNodeFinder = new IBGCoveringNodeFinder();
	
	/* true if the index is used somewhere in the graph */
	private final DefaultBitSet isUsed = new DefaultBitSet();
	
	/* Counter for assigning unique node IDs */
	private int nodeCount = 0;

	/* Temporary bit sets allocated once to allow reuse... only used in BuildNode */ 
	private final DefaultBitSet usedBitSet = new DefaultBitSet();
	private final DefaultBitSet childBitSet = new DefaultBitSet();
	
	/**
	 * Creates an IBG which is in a state ready for building.
	 * Specifically, the rootNode is physically constructed, but it is not
	 * expanded, so its cost and used set have not been determined.
	 * 
	 * In the initial state, the cost of the workload under the empty configuration
	 * is set, and may be accessed through emptyCost()
	 * 
	 * Nodes are built by calling buildNode() until it returns false.
     * @param conn
     *      an opened {@link DatabaseConnection connection}
     * @param sql
     *      a {@code sql query}.
     * @param candidateSet
     *      a set of candidate indexes.
     * @throws java.sql.SQLException
     *      an unexpected error has occurred.
     */
	public IndexBenefitGraphConstructor(DatabaseConnection<I> conn, String sql, Snapshot<I> candidateSet)
	throws SQLException {
		this.conn = conn;
		this.sql = sql;
		this.candidateSet = candidateSet;

		// set up the root node, and initialize the queue
		DefaultBitSet rootConfig = this.candidateSet.bitSet();
		rootNode = new IBGNode(rootConfig, nodeCount++);
		
		emptyCost = this.conn.getWhatIfOptimizer()
                        .whatIfOptimize(this.sql)
                        .using(new DefaultBitSet(), new DefaultBitSet())
                    .toGetCost();
		
		// initialize the queue
		queue.add(rootNode);
	}

    /**
     * @return cost of the workload under the empty configuration, stored in emptyCost.
     */
	public final double emptyCost() {
		return emptyCost;
	}

    /**
     * @return the {@link IBGNode root node}.
     */
	public final IBGNode rootNode() {
		return rootNode;
	}

    /**
     * @return the number of nodes that were constructed.
     */
	public final int nodeCount() {
		return nodeCount;
	}

    /**
     * @param i
     *      position of index in the bit set of used indexes.
     * @return {@code true} if the node is a used node.
     */
	public final boolean isUsed(int i) {
		return isUsed.get(i);
	}

    /**
     * @return a {@link Snapshot} of the set of candidate indexes.
     */
	public final Snapshot<I> candidateSet() {
		return candidateSet;
	}

	/**
	 * Wait for a specific node to be expanded
     * @param node
     *      a node to be expanded.
     */
	public final void waitUntilExpanded(IBGNode node) {
		synchronized (nodeExpansionMonitor) {
			while (!node.isExpanded())
				try {
					nodeExpansionMonitor.wait();
				} catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
				}
		}
	}
	
	/**
	 * Expands one node of the IBG. Returns true if a node was expanded, or false if there are no unexpanded nodes.
	 * This function is not safe to be called from more than one thread.
     * @return {@code true} if a node was expanded, or {@code false} if there are no unexpanded nodes.
     * @throws java.sql.SQLException
     *      unable to build the node.
     */
	public boolean buildNode() throws SQLException {
		IBGNode newNode, coveringNode;
		double totalCost;

		if (queue.isEmpty()) {
            return false;
        }
		
		newNode = queue.remove();
		
		// get cost and used set (stored into usedBitSet)
		usedBitSet.clear();
		coveringNode = coveringNodeFinder.find(rootNode, newNode.config);
		if (coveringNode != null) {
			totalCost = coveringNode.cost();
			coveringNode.addUsedIndexes(usedBitSet);
		} else {
			totalCost = conn.getWhatIfOptimizer()
                            .whatIfOptimize(sql)
                            .using(newNode.config, usedBitSet)
                        .toGetCost();
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
                final DefaultBitSet castBitset = Objects.as(childBitSet.clone());
				childNode = new IBGNode(castBitset, nodeCount++);
				queue.add(childNode);
			}
			childBitSet.set(u);
			
			IBGChild child = new IBGChild(childNode, u);
			if (firstChild == null) {
				firstChild = lastChild = child;
			} else {
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
	
	/**
	 * Auxiliary method for buildNodes which will find a built node or return null
     * if not found.
     * @param queue
     *      nodes queue.
     * @param config
     *      indexes configuration.
     * @return built node or return null
     *      if not found.
	 */
	private static IBGNode find(DefaultQueue<IBGNode> queue, DefaultBitSet config) {
		for (int i = 0; i < queue.count(); i++) {
			IBGNode node = queue.fetch(i);
			if (node.config.equals(config))
				return node;
		}
		return null;
	}

    /**
     * Sets a new empty cost for the empty configuration.
     * @param cost
     *      cost of the workload under the empty configuration, stored in emptyCost.
     */
	public void setEmptyCost(double cost) {
		emptyCost = cost;
	}

    /**
     * @return the {@link IndexBenefitGraph} object.
     */
	public IndexBenefitGraph getIBG() {
		return new IndexBenefitGraph(rootNode, emptyCost, isUsed);
	}
}


