package edu.ucsc.dbtune.ibg;

import edu.ucsc.dbtune.ibg.IndexBenefitGraph.IBGNode;
import edu.ucsc.dbtune.ibg.IndexBenefitGraph.IBGNode.IBGChild;
import edu.ucsc.dbtune.metadata.ConfigurationBitSet;
import edu.ucsc.dbtune.util.IndexBitSet;

/**
 * @author Karl Schnaitter
 */
public class IBGCoveringNodeFinder
{
    private final IndexBitSet visited = new IndexBitSet();
    private final IBGNodeStack  pending = new IBGNodeStack();

    /**
     * find the cost of a particular index configuration.
     *
     * @param ibg
     *      an {@link IndexBenefitGraph} object.
     * @param config
     *      an index configuration.
     * @return
     *      the cost of a particular index configuration. The return result is {@code null} if the 
     *      IBG is incomplete and no suitable covering node is found.
     */
    public final FindResult find(IndexBenefitGraph ibg, ConfigurationBitSet config)
    {
        if (config.isEmpty()) {
            return new FindResult(null, ibg.emptyCost());
        } else {
            final IBGNode foundNode = findFast(ibg.rootNode(), config.getBitSet(), null);

            if (foundNode != null) {
                // Obtain used indexes
                IndexBitSet usedBitSet = new IndexBitSet();
                foundNode.addUsedIndexes(usedBitSet);
                // Create the corresponding configuration
                ConfigurationBitSet usedConfiguration = new ConfigurationBitSet(config, usedBitSet);
                return new FindResult(usedConfiguration, foundNode.cost());
            } else {
                return new FindResult(null, 0.0);
            }
        }
    }
 
    /**
     * Returns an IBG node which matches a {@code guess}ed node given that the guessed node's actual 
     * configuration is a subset of the given used configuration.
     *
     * @param rootNode
     *      the {@code graph}'s root node.
     * @param config
     *      an index configuration.
     * @param guess
     *      a guessed {@link IBGNode}.
     * @return
     *     a found {@link IBGNode node}. <strong>IMPORTANT</strong>: this method may return
     *     {@code null} if the covering node is in an unexpanded part of the IBG.
     */
    public IBGNode findFast(IBGNode rootNode, IndexBitSet config, IBGNode guess)
    {
        visited.clear();

        IBGNode currentNode =
            (guess != null && guess.getConfiguration().contains(config)) ? guess : rootNode;

        while (true) {
            // stop if an unexpanded node is found. An unexpanded node means that the IBG 
            // construction hasn't finished yet, so whoever it's invoking this method will have to 
            // call it back again later
            if (!currentNode.isExpanded()) {
                return null;
            }

            IBGChild ch = currentNode.firstChild();

            while (true) {
                if (ch == null) {
                    return currentNode;
                } else if (!config.contains(ch.getUsedIndex())) {
                    currentNode = ch.getNode();
                    break;
                } else {
                    ch = ch.getNext();
                }
            }
        }
    }

    /**
     * find a particular node in the graph given the graph's root node and the index configuration.
     *
     * @param rootNode
     *      graph's root node.
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

            if (visited.contains(node.getID()))
                continue;

            visited.add(node.getID());

            // skip unexpanded nodes
            if (!node.isExpanded())
                continue;

            // we can prune the search if the node does not contain all of config
            if (!node.getConfiguration().contains(config))
                continue;

            // return if we have found covering node
            if (node.usedSetIsSubsetOf(config))
                return node;

            // this node has children that might be covering nodes...
            pending.addChildren(node.firstChild());
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
        private final ConfigurationBitSet usedConfiguration;
        
        private final double cost;
        
        /**
         * constructs a result.
         *
         * @param usedConfiguration
         *      the configuration
         * @param cost
         *      the cost
         */
        FindResult(ConfigurationBitSet usedConfiguration, double cost)
        {
            this.usedConfiguration  = usedConfiguration;
            this.cost               = cost;
        }

        /**
         * Gets the usedConfiguration for this instance.
         *
         * @return The usedConfiguration.
         */
        public ConfigurationBitSet getUsedConfiguration()
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
