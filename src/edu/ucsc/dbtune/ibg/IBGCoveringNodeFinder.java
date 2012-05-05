package edu.ucsc.dbtune.ibg;

import java.util.Deque;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Set;

import edu.ucsc.dbtune.metadata.Index;

/**
 * @author Karl Schnaitter
 * @author Ivo Jimenez
 */
public class IBGCoveringNodeFinder
{
    private final Set<IndexBenefitGraph.Node> visited = new HashSet<IndexBenefitGraph.Node>();
    private final Deque<IndexBenefitGraph.Node> pending = new LinkedList<IndexBenefitGraph.Node>();

    /**
     * find the cost of a particular index configuration in the given {@code ibg}.
     *
     * @param ibg
     *      an {@link IndexBenefitGraph} object.
     * @param config
     *      an index configuration.
     * @return
     *      the cost of a particular index configuration. The return result is {@code null} if the 
     *      IBG is incomplete and no suitable covering node is found.
     */
    public final FindResult find(IndexBenefitGraph ibg, Set<Index> config)
    {
        if (config.isEmpty())
            return new FindResult(new HashSet<Index>(), ibg.emptyCost());

        final IndexBenefitGraph.Node foundNode = find(ibg.rootNode(), config);

        if (foundNode == null)
            return null;

        Set<Index> used = new HashSet<Index>();
        used.addAll(foundNode.getUsedIndexes());
        return new FindResult(used, foundNode.cost());
    }

    /**
     * find a particular node in the graph given the graph's root node and the index configuration.
     *
     * @param rootNode
     *      graph's root node.
     * @param config
     *      index configuration.
     * @return
     *      found node in the graph. <strong>IMPORTANT</strong>: may return {@code null}, if the 
     *      node for the given configuration wasn't found.
     */
    public IndexBenefitGraph.Node find(IndexBenefitGraph.Node rootNode, Set<Index> config)
    {
        visited.clear();
        pending.clear();

        pending.add(rootNode);

        IndexBenefitGraph.Node node;

        while ((node = pending.poll()) != null) {

            if (visited.contains(node))
                continue;

            visited.add(node);

            // skip unexpanded nodes
            if (!node.isExpanded())
                continue;

            // we can prune the search if the node does not contain all of config
            if (!node.getConfiguration().containsAll(config))
                continue;

            // return if we have found a covering node
            if (config.containsAll(node.getUsedIndexes()))
                return node;

            // this node has children that might be covering nodes...
            pending.addAll(node.getChildren());
        }

        return null;
    }

    /**
     * The result of a {@link IBGCoveringNodeFinder#find} invokation.
     *
     * @author Alkis Polyzotis
     */
    public class FindResult
    {
        private final Set<Index> usedConfiguration;
        
        private final double cost;
        
        /**
         * constructs a result.
         *
         * @param usedConfiguration
         *      the configuration
         * @param cost
         *      the cost
         */
        FindResult(Set<Index> usedConfiguration, double cost)
        {
            this.usedConfiguration  = usedConfiguration;
            this.cost               = cost;
        }

        /**
         * Gets the usedConfiguration for this instance.
         *
         * @return The usedConfiguration.
         */
        public Set<Index> getUsedConfiguration()
        {
            return this.usedConfiguration;
        }

        /**
         * Gets the cost for this instance.
         *
         * @return The cost.
         */
        public double getCost()
        {
            return this.cost;
        }
    }
}
