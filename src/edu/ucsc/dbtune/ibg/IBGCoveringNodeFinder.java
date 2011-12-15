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
    private final IndexBitSet visited;
    private final IBGNodeStack  pending;

    /**
     * construct a new {@link IBGCoveringNodeFinder} object; assuming some default values for
     * its private members.
     */
    public IBGCoveringNodeFinder()
    {
        this(new IndexBitSet(), new IBGNodeStack());
    }

    /**
     * construct a new {@link IBGCoveringNodeFinder} object given some {@link 
     * edu.ucsc.dbtune.util.IndexBitSet visited nodes} and a {@link IBGNodeStack pending stack} of
     * nodes.
     *
     * @param visited visited nodes
     * @param pending pending stack of nodes.
     */
    IBGCoveringNodeFinder(IndexBitSet visited, IBGNodeStack pending)
    {
        this.visited = visited;
        this.pending = pending;
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
    
    /**
     * find the cost of a particular indexes configuration.
     * @param ibg
     *      an {@link IndexBenefitGraph} object.
     * @param config
     *      an indexes configuration.
     * @return
     *      the cost of a particular indexes configuration. The return result is {@code null} if 
     *      the IBG is incomplete and no suitable covering node is found.
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
     * find the cost of a particular indexes configuration in multiple graphs.
     * @param ibgs
     *      multiple {@link IndexBenefitGraph} object.
     * @param config
     *      an indexes configuration.
     * @return
     *      the cost of a particular indexes configuration.
     */
    public final double findCost(IndexBenefitGraph[] ibgs, ConfigurationBitSet config)
    {
        double cost = 0;
        for (IndexBenefitGraph ibg : ibgs) {
            cost += find(ibg, config).cost;
        }
        return cost;
    }

    /**
     * Returns an {@code ibg node} which matches a {@code guessed node} given
     * that the {@code guessed node}'s actual configuration is a subset of currently used
     * configuration.
     * @param rootNode
     *      the {@code graph}'s root node.
     * @param config
     *      an index configuration.
     * @param guess
     *      a guessed {@link IBGNode}.
     * @return
     *     an found {@link IBGNode node}. <strong>IMPORTANT</strong>: this method may return
     *     {@code null} if the covering node is in an unexpanded part of the IBG.
     */
    public IBGNode findFast(IBGNode rootNode, IndexBitSet config, IBGNode guess)
    {
        visited.clear();

        IBGNode currentNode =
            (guess != null && config.subsetOf(guess.getConfiguration())) ? guess : rootNode;

        while (true) {
            // stop if an unexpanded node is found
            if (!currentNode.isExpanded()) {
                return null;
            }
            IBGChild ch = currentNode.firstChild();
            while (true) {
                if (ch == null) {
                    return currentNode;
                } else if (!config.get(ch.getUsedIndex())) {
                    currentNode = ch.getNode();
                    break;
                } else {
                    ch = ch.getNext();
                }
            }
        }
    }

    /**
     * find a particular node in the graph given the graph's root node and the indexes
     * configuration.
     * @param rootNode
     *      graph's root node.
     * @param config
     *      indexes configuration.
     * @return
     *      found node in the graph.
     */
    public IBGNode find(IBGNode rootNode, IndexBitSet config)
    {
        visited.clear();
        pending.clear();

        pending.addNode(rootNode);
        while (pending.hasNext()) {
            IBGNode node = pending.next();

            if (visited.get(node.getID())) {
                continue;
            }
            visited.set(node.getID());

            // skip unexpanded nodes
            if (!node.isExpanded()) {
                continue;
            }

            // prune non-supersets
            if (!config.subsetOf(node.getConfiguration())) {
                continue;
            }

            // return if we have found covering node
            if (node.usedSetIsSubsetOf(config)) {
                return node;
            }

            // this node has children that might be covering nodes... continue on
            pending.addChildren(node.firstChild());
        }

        return null;
    }

    /**
     *
    public void find(IBGNode rootNode, IndexBitSet[] configs, int configCount, IBGNode[] outNodes)
    {
        for (int i = 0; i < configCount; i++) {
            assert configs[i] != null;
            outNodes[i] = null;
        }

        visited.clear();
        pending.clear();

        pending.addNode(rootNode);
        while (pending.hasNext()) {
            IBGNode node = pending.next();

            if (visited.get(node.getID())) {
                continue;
            }
            visited.set(node.getID());

            if (!node.isExpanded()) {
                continue;
            }

            boolean missingCoveringNode = false;
            boolean supersetOfMissing = false;
            for (int i = 0; i < configCount; i++) {
                if (outNodes[i] == null) {
                    boolean subset = configs[i].subsetOf(node.getConfiguration());
                    boolean containsUsed = node.usedSetIsSubsetOf(configs[i]);
                    if (subset && containsUsed) {
                        outNodes[i] = node;
                    } else {
                        missingCoveringNode = true;
                        supersetOfMissing = supersetOfMissing || subset;
                    }
                }
            }

            if (!missingCoveringNode) {
                return;
            }

            if (supersetOfMissing) {
                pending.addChildren(node.firstChild());
            }
        }
    }
     */
}
