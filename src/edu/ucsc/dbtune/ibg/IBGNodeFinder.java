package edu.ucsc.dbtune.ibg;

import edu.ucsc.dbtune.ibg.IndexBenefitGraph.IBGNode;
import edu.ucsc.dbtune.util.IndexBitSet;

/**
 * Does a search for a particular node in the graph.
 *
 * @author Karl Schnaitter
 */
class IBGNodeFinder
{
    private final IndexBitSet visited = new IndexBitSet();
    private final IBGNodeStack  pending = new IBGNodeStack();

    /**
     * finds a particular node in the graph.
     *
     * @param rootNode
     *      {@link IndexBenefitGraph}'s root.
     * @param config
     *      index configuration.
     * @return
     *      found node in the graph. <strong>IMPORTANT</strong>: may return {@code null}.
     */
    public IBGNode find(IBGNode rootNode, IndexBitSet config)
    {
        visited.clear();
        pending.clear();
        
        pending.addNode(rootNode);

        while (pending.hasNext()) {
            IBGNode node = pending.next();
            
            if (visited.get(node.getID()))
                continue;

            visited.set(node.getID());
            
            // skip unexpanded nodes
            if (!node.isExpanded())
                continue;

            // we can prune the search if the node does not contain all of config
            if (!config.subsetOf(node.getConfiguration()))
                continue;
            
            // we can stop the search if the node matches exactly
            if (node.getConfiguration().equals(config))
                return node;
            
            // this node has children that might be covering nodes...
            pending.addChildren(node.firstChild());
        }   
        
        return null;
    }
}
