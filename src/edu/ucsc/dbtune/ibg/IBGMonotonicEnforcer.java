package edu.ucsc.dbtune.ibg;

import edu.ucsc.dbtune.ibg.IndexBenefitGraph.IBGNode;
import edu.ucsc.dbtune.util.IndexBitSet;

public class IBGMonotonicEnforcer
{
    private final IndexBitSet visited;
    private final IBGNodeQueue  pending;
    private final SubSearch     sub;

    /**
     * construct an {@link IBGMonotonicEnforcer} object.
     */
    public IBGMonotonicEnforcer()
    {
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
    public void fix(IndexBenefitGraph ibg)
    {
        visited.clear();
        pending.clear();
        
        pending.addNode(ibg.rootNode());
        while (pending.hasNext()) {
            IBGNode node = pending.next();
            
            if (visited.contains(node.getID())){
                continue;
            }
            
            visited.add(node.getID());
            
            sub.fixSubsets(ibg, node.getConfiguration(), node.cost());
            
            if (node.isExpanded()) {
                if (node.firstChild() == null) {
                    if (node.cost() > ibg.emptyCost()) {
                        ibg.setEmptyCost(node.cost());
                    }
                } else{
                    pending.addChildren(node.firstChild());
                }
            }
        }   
    }

    /**
     * a Subsearch space in the {@link IndexBenefitGraph}.
     */
    private static class SubSearch
    {
        private final IndexBitSet visited = new IndexBitSet();
        private final IBGNodeStack pending = new IBGNodeStack();
        
        private void fixSubsets(IndexBenefitGraph ibg, IndexBitSet config, double cost)
        {
            visited.clear();
            pending.clear();
            
            pending.addNode(ibg.rootNode());
            while (pending.hasNext()) {
                IBGNode node = pending.next();
                
                if (visited.contains(node.getID())){
                    continue;
                }

                visited.add(node.getID());
                
                if (config.containsAll(node.getConfiguration()) && 
                        !node.getConfiguration().equals(config)) {
                    if (node.cost() < cost) {
                        node.setCost(cost);
                    }
                }
                else if (node.isExpanded()) 
                    pending.addChildren(node.firstChild());
            }       
        }
    }
}
