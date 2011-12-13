package edu.ucsc.dbtune.ibg;

import edu.ucsc.dbtune.ibg.IndexBenefitGraph.IBGNode;
import edu.ucsc.dbtune.util.IndexBitSet;

/**
 * IBGNodeFinder -- does a search for a particular node in the graph
 */
class IBGNodeFinder
{
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
            
            if (visited.get(node.getID())) {
                continue;
            }
            visited.set(node.getID());
            
            // we can prune the search if the node does not contain all of config
            if (!config.subsetOf(node.getConfiguration())) {
                continue;
            }
            
            // we can stop the search if the node matches exactly
            if (node.getConfiguration().equals(config)) {
                return node;
            }
            
            if (node.isExpanded()) {
                pending.addChildren(node.firstChild());
            }
        }   
        
        return null;
    }
}
