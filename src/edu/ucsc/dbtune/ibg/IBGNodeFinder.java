package edu.ucsc.dbtune.ibg;

import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.util.BitArraySet;

/**
 * Does a search for a particular node in the graph.
 *
 * @author Karl Schnaitter
 */
class IBGNodeFinder
{
    private final BitArraySet<IndexBenefitGraph.Node> visited =
        new BitArraySet<IndexBenefitGraph.Node>();
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
    public IndexBenefitGraph.Node find(
            IndexBenefitGraph.Node rootNode, BitArraySet<Index> config)
    {
        visited.clear();
        pending.clear();
        
        pending.addNode(rootNode);

        while (pending.hasNext()) {
            IndexBenefitGraph.Node node = pending.next();
            
            if (visited.contains(node))
                continue;

            visited.add(node);
            
            // skip unexpanded nodes
            if (!node.isExpanded())
                continue;

            // we can prune the search if the node does not contain all of config
            if (!node.getConfiguration().containsAll(config))
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
