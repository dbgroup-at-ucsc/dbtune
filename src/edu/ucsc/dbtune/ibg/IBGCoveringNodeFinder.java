package edu.ucsc.dbtune.ibg;

import java.util.Set;

import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.util.BitArraySet;

/**
 * @author Karl Schnaitter
 * @author Ivo Jimenez
 */
public class IBGCoveringNodeFinder
{
    private final Set<IndexBenefitGraph.Node> visited = new BitArraySet<IndexBenefitGraph.Node>();
    private final IBGNodeStack  pending = new IBGNodeStack();

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
        if (config.isEmpty()) {
            return new FindResult(null, ibg.emptyCost());
        } else {
            final IndexBenefitGraph.Node foundNode = findFast(ibg.rootNode(), config, null);

            if (foundNode != null) {
                // Obtain used indexes
                Set<Index> usedBitSet = new BitArraySet<Index>();
                foundNode.addUsedIndexes(usedBitSet);
                // Create the corresponding configuration
                Set<Index> usedConfiguration = new BitArraySet<Index>(usedBitSet);
                return new FindResult(usedConfiguration, foundNode.cost());
            } else {
                return new FindResult(null, 0.0);
            }
        }
    }
 
    /**
     * Returns an IBG node, assuming that a guessed node's actual configuration is a superset of the 
     * given configuration. That is, if {@latex.inline config $\\triangleleft$ guessed}, the guessed 
     * node is used as the root from which the search begins. If it isn't, {@code root} is used to 
     * begin the search, which is equivalent to invoking {@code findFast(root, conf, null)} or 
     * {@code find(root,conf)}.
     *
     * @param root
     *      the {@code graph}'s root node.
     * @param conf
     *      an index configuration.
     * @param guess
     *      a guessed {@link Node}. Might be null if the guessing is intended to begin at the 
     *      root.
     * @return
     *     a found {@link Node node}. <strong>IMPORTANT</strong>: this method may return
     *     {@code null} if the covering node is in an unexpanded part of the IBG.
     */
    public IndexBenefitGraph.Node findFast(
            IndexBenefitGraph.Node root,
            Set<Index> conf,
            IndexBenefitGraph.Node guess)
    {
        visited.clear();

        IndexBenefitGraph.Node currentNode =
            (guess != null && guess.getConfiguration().containsAll(conf)) ? guess : root;

        while (true) {
            // stop if an unexpanded node is found. An unexpanded node means that the IBG 
            // construction hasn't finished yet, so whoever it's invoking this method will have to 
            // call it back again later
            if (!currentNode.isExpanded()) {
                return null;
            }

            IndexBenefitGraph.Node.Child ch = currentNode.firstChild();

            while (true) {
                if (ch == null) {
                    return currentNode;
                } else if (!conf.contains(ch.getUsedIndex())) {
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
    public IndexBenefitGraph.Node find(
            IndexBenefitGraph.Node rootNode,
            Set<Index> config)
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
