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

import edu.ucsc.dbtune.ibg.IndexBenefitGraph.IBGNode;
import edu.ucsc.dbtune.util.DefaultBitSet;

public class IBGAnalyzer {
	// the IBG we are currently exploring
	protected final IndexBenefitGraphConstructor<?> ibgCons;

	// queue of nodes to explore
	private final IBGNodeQueue nodeQueue;

	// queue of nodes to revisit
	private final IBGNodeQueue revisitQueue;

	// the set of all used indexes seen so far in the IBG
	// this is different from the bank, because that may have
	// used indexes from other IBG traversals
	private final DefaultBitSet allUsedIndexes;

	// graph traversal objects
	private IBGCoveringNodeFinder coveringNodeFinder = new IBGCoveringNodeFinder();

	// copy of the root bit set for convenience
	private final DefaultBitSet rootBitSet;

	// keeps track of visited nodes
	private final DefaultBitSet visitedNodes;

    /**
     * construct an {@code IBGAnalyzer}.
     * @param ibgCons
     *      a given {@link IndexBenefitGraphConstructor} object.
     * @param nodeQueue
     *      a given {@link IBGNodeQueue} object which contains IBG nodes.
     * @param revisitQueue
     *      a given {@link IBGNodeQueue} object which contains IBG node that will be revisited.
     */
    public IBGAnalyzer(IndexBenefitGraphConstructor<?> ibgCons, IBGNodeQueue nodeQueue, IBGNodeQueue revisitQueue){
		// initialize fields
		this.ibgCons = ibgCons;
		this.nodeQueue = nodeQueue;
		this.revisitQueue = revisitQueue;
		allUsedIndexes = new DefaultBitSet();
		rootBitSet = ibgCons.rootNode().config.clone();
		visitedNodes = new DefaultBitSet();

		// seed the queue with the root node
		nodeQueue.addNode(ibgCons.rootNode());
    }

    /**
     * construct an {@code IBGAnalyzer}.
     * @param ibgCons
     *      a given {@link IndexBenefitGraphConstructor} object.
     */
	public IBGAnalyzer(IndexBenefitGraphConstructor<?> ibgCons) {
        this(ibgCons, new IBGNodeQueue(), new IBGNodeQueue());
	}

    /**
     * traverses the {@link IndexBenefitGraph}.
     * @param logger
     *      a logger that keeps tracks of the visited nodes.
     * @param wait
     *      a flag that indicates if {@link IndexBenefitGraphConstructor}
     *      should wait ({@code true}) for a node in the graph to expand
     *      before doing something.
     * @return either {@link StepStatus#BLOCKED}, {@link StepStatus#DONE}, or
     *      {@link StepStatus#SUCCESS}.
     */
	public final StepStatus analysisStep(InteractionLogger logger, boolean wait) {
		// we might need to go through several nodes to find one that we haven't visited yet
		while (true) {
			IBGNode node;
			
			if (nodeQueue.hasNext()) {
				if (nodeQueue.peek().isExpanded()) { 
					node = nodeQueue.next();
				} else if (wait) {
					node = nodeQueue.next();
					ibgCons.waitUntilExpanded(node);
				} else if (revisitQueue.hasNext()) {
					node = revisitQueue.next();
				} else {
					return StepStatus.BLOCKED;
				}
			} else if (revisitQueue.hasNext()) {
				node = revisitQueue.next();
			} else {
				return StepStatus.DONE;
			}
			
			if (visitedNodes.get(node.id)){
                continue;
            }
			
			if (analyzeNode(node, logger)) {
				// node is done ... move on to children
				visitedNodes.set(node.id);
				nodeQueue.addChildren(node.firstChild());
			} else {
				revisitQueue.addNode(node);
			}
			
			if (revisitQueue.hasNext() || nodeQueue.hasNext()) {
                return StepStatus.SUCCESS;
            }
		}
	}


	/**
     * analysis a specific node in the {@link IndexBenefitGraph graph}.
	 * @param node
     *      an {@link IBGNode node} in the graph.
     * @param logger
     *      an {@link InteractionLogger graph logger}.
     * @return {@code true} if the analysis was successful.
	 */
	private boolean analyzeNode(IBGNode node, InteractionLogger logger) {
        return new IBGAnalyzerWorkspace(
                node,
                logger,
                rootBitSet,
                allUsedIndexes
        ).runAnalysis(coveringNodeFinder, ibgCons);
	}


    /**
	 * Perform one step of analysis
	 * 
	 * Return true if there might be some work left to do for this analysis
	 */
	public enum StepStatus {
        /**
         * there was no expanded node to analyze, and there is an unexpanded node
         */
		BLOCKED,

        /**
         * all nodes are analyzed
         */
		DONE,

        /**
         * there was an expanded node to analyze
         */
		SUCCESS
	}
}

