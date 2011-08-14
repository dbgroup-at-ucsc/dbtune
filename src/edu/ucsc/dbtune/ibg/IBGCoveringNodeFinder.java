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

import edu.ucsc.dbtune.ibg.IndexBenefitGraph.IBGChild;
import edu.ucsc.dbtune.ibg.IndexBenefitGraph.IBGNode;
import edu.ucsc.dbtune.util.IndexBitSet;
import edu.ucsc.dbtune.util.ToStringBuilder;

public class IBGCoveringNodeFinder {
    private final IndexBitSet visited;
    private final IBGNodeStack  pending;

    private static final double ZERO_COST = 0.0;

    /**
     * construct a new {@link IBGCoveringNodeFinder} object; assuming some default values for
     * its private members.
     */
    public IBGCoveringNodeFinder(){
        this(new IndexBitSet(), new IBGNodeStack());
    }

    /**
     * construct a new {@link IBGCoveringNodeFinder} object given some
     * {@link edu.ucsc.dbtune.util.IndexBitSet visited nodes} and a {@link IBGNodeStack pending stack} of
     * nodes.
     * @param visited visited nodes
     * @param pending pending stack of nodes.
     */
    IBGCoveringNodeFinder(IndexBitSet visited, IBGNodeStack pending){
        this.visited = visited;
        this.pending = pending;
    }

    /**
     * find the cost of a particular indexes configuration.
     * @param ibg
     *      an {@link IndexBenefitGraph} object.
     * @param config
     *      an indexes configuration.
     * @return
     *      the cost of a particular indexes configuration.
     */
    public final double findCost(IndexBenefitGraph ibg, IndexBitSet config) {
        if (config.isEmpty()) {
            return ibg.emptyCost();
    } else {
      final IBGNode foundNode = findFast(ibg.rootNode(), config, null);
            return foundNode != null ? foundNode.cost() : ZERO_COST;
    }
    }

    /**
     * find the cost of a particular indexes configuration in multiple graphs.
     * @param ibgs
     *      multiple {@link IndexBenefitGraph} object.
     * @param config
     *      an indexes configuration.
     * @return
     *      the cost of a particular indexes configuration.
     */
    public final double findCost(IndexBenefitGraph[] ibgs, IndexBitSet config) {
        double cost = 0;
        for (IndexBenefitGraph ibg : ibgs){
            cost += findCost(ibg, config);
    }
        return cost;
    }

    /**
     * Returns an {@code ibg node} which matches a {@code guessed node} given
     * that the {@code guessed node}'s actual configuration is a subset of currently used
     * configuration.
     * @param rootNode
     *      the {@code graph}'s root node.
     * @param config
     *      an index configuration.
     * @param guess
     *      a guessed {@link IBGNode}.
     * @return
     *     an found {@link IBGNode node}. <strong>IMPORTANT</strong>: this method may return
     *     {@code null}.
     */
    public IBGNode findFast(IBGNode rootNode, IndexBitSet config, IBGNode guess) {
        visited.clear(); // not using it, but clear it anyway?
        
        IBGNode currentNode = (guess != null && config.subsetOf(guess.config)) ? guess : rootNode;
        while (true) {
            // stop if an unexpanded node is found
            if (!currentNode.isExpanded()){
        return null;
      }
            
            IBGChild ch = currentNode.firstChild();
            while (true) {
                if (ch == null) {
                    return currentNode;
                } else if (!config.get(ch.usedIndex)) {
                    currentNode = ch.node; 
                    break;
                } else {
                    ch = ch.next;
                }
            }
        }
    }

    /**
     * find a particular node in the graph given the graph's root node and the indexes
     * configuration.
     * @param rootNode
     *      graph's root node.
     * @param config
     *      indexes configuration.
     * @return
     *      found node in the graph.
     */
    public IBGNode find(IBGNode rootNode, IndexBitSet config) {
        visited.clear();
        pending.reset();
        
        pending.addNode(rootNode);
        while (pending.hasNext()) {
            IBGNode node = pending.next();
            
            if (visited.get(node.id)) {
        continue;
      }
            visited.set(node.id);
            
            // skip unexpanded nodes
            if (!node.isExpanded()){
        continue;
      }
            
            // prune non-supersets
            if (!config.subsetOf(node.config)){
        continue;
      }
            
            // return if we have found covering node
            if (node.usedSetIsSubsetOf(config)){
        return node;
      }
            
            // this node has children that might be covering nodes... continue on
            pending.addChildren(node.firstChild());
        }
        
        return null;
    }

    // todo(Huascar) what's the purpose of this method?
    public void find(IBGNode rootNode, IndexBitSet[] configs, int configCount, IBGNode[] outNodes) {
        for (int i = 0; i < configCount; i++) {
            assert(configs[i] != null);
            outNodes[i] = null;
        }
        
        visited.clear();
        pending.reset();
        
        pending.addNode(rootNode);
        while (pending.hasNext()) {
            IBGNode node = pending.next();
            
            if (visited.get(node.id)) {
          continue;
      }
            visited.set(node.id);
            
            if (!node.isExpanded()){
        continue;
      }

            boolean missingCoveringNode = false;
            boolean supersetOfMissing = false;
            for (int i = 0; i < configCount; i++) {
                if (outNodes[i] == null) {
                    boolean subset = configs[i].subsetOf(node.config);
                    boolean containsUsed = node.usedSetIsSubsetOf(configs[i]);
                    if (subset && containsUsed) {
                        outNodes[i] = node;
                    } else {
                        missingCoveringNode = true;
                        supersetOfMissing = supersetOfMissing || subset;
                    }
                }
            }

            if (!missingCoveringNode){
          return;
      }
            if (supersetOfMissing){
          pending.addChildren(node.firstChild());
      }
        }
    }

    @Override
    public String toString() {
        return new ToStringBuilder<IBGCoveringNodeFinder>(this)
               .add("visited nodes", visited)
               .add("pending nodes", pending)
               .toString();
    }
}
