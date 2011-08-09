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
import edu.ucsc.dbtune.util.ToStringBuilder;

public class IBGBestBenefitFinder {
	private final IndexBitSet visited;
	private final IndexBitSet bitset_Ya;
	private final IBGNodeQueue          pending;
	private final IBGCoveringNodeFinder finder;

    /**
     * construct a new {@link IBGBestBenefitFinder} object; assumming default values for its private members.
     */
    public IBGBestBenefitFinder(){
        this(new IndexBitSet(), new IndexBitSet(), new IBGNodeQueue(), new IBGCoveringNodeFinder());
    }

    /**
     * convenient constructor that will help us test this class.
     * @param visited visited indexes
     * @param bitset_Ya new indexes to be processed.
     * @param pendingQueue pending node queue.
     * @param finder  covering node finder.
     */
    IBGBestBenefitFinder(IndexBitSet visited, IndexBitSet bitset_Ya, IBGNodeQueue pendingQueue, IBGCoveringNodeFinder finder){
        this.visited = visited;
        this.bitset_Ya = bitset_Ya;
        this.pending   = pendingQueue;
        this.finder = finder;
    }

    /**
     * calculates the best benefit for a given index in the index benefit graph.
     * @param ibg
     *      the {@link IndexBenefitGraph}.
     * @param indexId
     *      an {@link edu.ucsc.dbtune.core.Index}'s unique identifier.
     * @param M
     *      an index configuration M.
     * @return
     *     the best benefit for a given index in the index benefit graph.
     */
	public double bestBenefit(IndexBenefitGraph ibg, int indexId, IndexBitSet M) {
		visited.clear();
		pending.reset();
		
		double bestValue = 0;
		
		pending.addNode(ibg.rootNode());
		while (pending.hasNext()) {
			IBGNode Y = pending.next();

			if (visited.get(Y.id)) 
				continue;
			visited.set(Y.id);

			if (!Y.config.get(indexId) && M.subsetOf(Y.config)) {
				bitset_Ya.set(Y.config);
				bitset_Ya.set(indexId);
				IBGNode Ya = finder.findFast(ibg.rootNode(), bitset_Ya, null);
				double value = Y.cost() - Ya.cost();
				bestValue = Math.max(value, bestValue);
			}
			pending.addChildren(Y.firstChild());
		}
		
		return bestValue;
	}

    @Override
    public String toString() {
        return new ToStringBuilder<IBGBestBenefitFinder>(this)
               .add("visited",visited)
               .add("bitset_Ya",bitset_Ya)
               .add("pending",pending)
               .add("finder",finder)
               .toString();
    }
}
