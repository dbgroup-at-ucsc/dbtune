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

public class IBGMonotonicEnforcer {
    private final IndexBitSet visited;
    private final IBGNodeQueue  pending;
    private final SubSearch     sub;

    /**
     * construct an {@link IBGMonotonicEnforcer} object.
     */
    public IBGMonotonicEnforcer(){
        this(new IndexBitSet(), new IBGNodeQueue(), new SubSearch());
    }

    /**
     * construct an {@link IBGMonotonicEnforcer} object.
     * @param visited
     *      visited nodes in the graph.
     * @param pending
     *      queue of pending {@link IBGNode}.
     * @param sub
     *      a {@link SubSearch} space.
     */
    IBGMonotonicEnforcer(IndexBitSet visited, IBGNodeQueue pending, SubSearch sub){
        this.visited = visited;
        this.pending = pending;
        this.sub = sub;
    }

    /**
     * Fixes the {@link IndexBenefitGraph}.
     * @param ibg
     *      the {@link IndexBenefitGraph} to be fixed.
     */
    public void fix(IndexBenefitGraph ibg) {
        visited.clear();
        pending.reset();
        
        pending.addNode(ibg.rootNode());
        while (pending.hasNext()) {
            IBGNode node = pending.next();
            
            if (visited.get(node.id)){
                continue;
            }
            
            visited.set(node.id);
            
            sub.fixSubsets(ibg, node.config, node.cost());
            
            if (node.isExpanded()) {
                if (node.firstChild() == null) {
                    if (node.cost() > ibg.emptyCost()) {
                        System.out.flush();
                        System.err.printf("monotonicity violation: %f%% ", 100.0*(node.cost() - ibg.emptyCost())/node.cost());
                        System.err.flush();
                        ibg.setEmptyCost(node.cost());
                    }
                } else{
                    pending.addChildren(node.firstChild());
                }
            }
        }   
    }


    @Override
    public String toString() {
        return new ToStringBuilder<IBGMonotonicEnforcer>(this)
               .add("visited indexes", visited)
               .add("pending queue", pending)
               .add("subsearch space", sub)
               .toString();
    }

    /**
     * a Subsearch space in the {@link IndexBenefitGraph}.
     */
    private static class SubSearch {
        private final IndexBitSet visited = new IndexBitSet();
        private final IBGNodeStack pending = new IBGNodeStack();
        
        private void fixSubsets(IndexBenefitGraph ibg, IndexBitSet config, double cost) {
            visited.clear();
            pending.reset();
            
            pending.addNode(ibg.rootNode());
            while (pending.hasNext()) {
                IBGNode node = pending.next();
                
                if (visited.get(node.id)){
                    continue;
                }

                visited.set(node.id);
                
                if (node.config.subsetOf(config) && !node.config.equals(config)) {
                    if (node.cost() < cost) {
                        System.out.flush();
                        System.err.printf("monotonicity violation: %f%% ", 100.0*(node.cost() - cost)/node.cost());
                        System.err.flush();
                        node.setCost(cost);
                    }
                }
                else if (node.isExpanded()) 
                    pending.addChildren(node.firstChild());
            }       
        }

        @Override
        public String toString() {
            return new ToStringBuilder<SubSearch>(this)
                   .add("visited indexes", visited)
                   .add("pending stack", pending)
                   .toString();
        }
    }
}
