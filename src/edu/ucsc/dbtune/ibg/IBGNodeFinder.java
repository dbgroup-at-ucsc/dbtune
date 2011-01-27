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
import edu.ucsc.dbtune.util.IndexBitSet;

/**
 * IBGNodeFinder -- does a search for a particular node in the graph
 */
class IBGNodeFinder {
	private final IndexBitSet visited = new IndexBitSet();
	private final IBGNodeStack  pending = new IBGNodeStack();

    /**
     * finds a particular node in the graph.
     * @param rootNode
     *      {@link IndexBenefitGraph}'s root.
     * @param config
     *      indexes configuration.
     * @return
     *      found node in the graph. <strong>IMPORTANT</strong>: this method
     *      may return {@code null}.
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
			
			// we can prune the search if the node does not contain all of config
			if (!config.subsetOf(node.config)) {
                continue;
            }
			
			// we can stop the search if the node matches exactly
			if (node.config.equals(config)) {
                return node;
            }
			
			if (node.isExpanded()) {
                pending.addChildren(node.firstChild());
            }
		}	
		
		return null;
	}
}
