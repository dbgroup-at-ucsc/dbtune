package edu.ucsc.dbtune.ibg;

import edu.ucsc.dbtune.advisor.interactions.InteractionLogger;
import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.util.BitArraySet;

/**
 * This class implements the qINTERACT algorithm described in Schnaitter et. al. for computing the 
 * degree of interaction for all the pairs {@latex.inline $a,b \\in S$}
 *
 * @author Karl Schnaitter
 * @see <a href="http://portal.acm.org/citation.cfm?id=1687766">
 *     Index interactions in physical design tuning: modeling, analysis, and applications</a>
 */
public class IBGAnalyzer
{
    // the IBG we are currently exploring
    protected final IndexBenefitGraphConstructor ibgCons;

    // queue of nodes to explore
    private final IBGNodeQueue nodeQueue;

    // queue of nodes to revisit
    private final IBGNodeQueue revisitQueue;

    // the set of all used indexes seen so far in the IBG
    // this is different from the bank, because that may have
    // used indexes from other IBG traversals
    private final BitArraySet<Index> allUsedIndexes;

    // graph traversal objects
    private IBGCoveringNodeFinder coveringNodeFinder = new IBGCoveringNodeFinder();

    // copy of the root bit set for convenience
    private final BitArraySet<Index> rootBitSet;

    // keeps track of visited nodes
    private final BitArraySet<IndexBenefitGraph.Node> visitedNodes;

    // All the following correspond to a bunch of structures that we keep around to
    // avoid excessive garbage collection. These structures are only used in the
    // analyzeNode() method.
    private BitArraySet<Index> candidatesBitSet = new BitArraySet<Index>();
    private BitArraySet<Index> usedBitSet = new BitArraySet<Index>();
    private BitArraySet<Index> bitsetYaSimple = new BitArraySet<Index>();
    private BitArraySet<Index> bitsetYa = new BitArraySet<Index>();
    private BitArraySet<Index> bitsetYbMinus = new BitArraySet<Index>();
    private BitArraySet<Index> bitsetYbPlus = new BitArraySet<Index>();
    private BitArraySet<Index> bitsetYab = new BitArraySet<Index>();

    /**
     * construct an {@code IBGAnalyzer}.
     *
     * @param ibgCons
     *      a given {@link IndexBenefitGraphConstructor} object.
     * @param nodeQueue
     *      a given {@link IBGNodeQueue} object which contains IBG nodes.
     * @param revisitQueue
     *      a given {@link IBGNodeQueue} object which contains IBG node that will be revisited.
     */
    public IBGAnalyzer(IndexBenefitGraphConstructor ibgCons, IBGNodeQueue nodeQueue, IBGNodeQueue 
            revisitQueue)
    {
        // initialize fields
        this.ibgCons        = ibgCons;
        this.nodeQueue      = nodeQueue;
        this.revisitQueue   = revisitQueue;
        allUsedIndexes      = new BitArraySet<Index>();
        rootBitSet          = new BitArraySet<Index>(ibgCons.rootNode().getConfiguration());
        visitedNodes        = new BitArraySet<IndexBenefitGraph.Node>();

        // seed the queue with the root node
        nodeQueue.addNode(ibgCons.rootNode());
    }

    /**
     * construct an {@code IBGAnalyzer}.
     *
     * @param ibgCons
     *      a given {@link IndexBenefitGraphConstructor} object.
     */
    public IBGAnalyzer(IndexBenefitGraphConstructor ibgCons)
    {
        this(ibgCons, new IBGNodeQueue(), new IBGNodeQueue());
    }

    /**
     * traverses the {@link IndexBenefitGraph}.
     *
     * @param logger
     *      a logger that keeps tracks of the visited nodes.
     * @param wait
     *      a flag that indicates if {@link IndexBenefitGraphConstructor}
     *      should wait ({@code true}) for a node in the graph to expand
     *      before doing something.
     * @return either {@link StepStatus#BLOCKED}, {@link StepStatus#DONE}, or
     *      {@link StepStatus#SUCCESS}.
     */
    public final StepStatus analysisStep(InteractionLogger logger, boolean wait)
    {
        // we might need to go through several nodes to find one that we haven't visited yet
        while (true) {
            IndexBenefitGraph.Node node;

            if (nodeQueue.hasNext()) {
                if (nodeQueue.peek().isExpanded()) {
                    node = nodeQueue.next();
                }
                else if (wait) {
                    node = nodeQueue.next();
                    ibgCons.waitUntilExpanded(node);
                }
                else if (revisitQueue.hasNext()) {
                    node = revisitQueue.next();
                }
                else {
                    return StepStatus.BLOCKED;
                }
            }
            else if (revisitQueue.hasNext()) {
                node = revisitQueue.next();
            }
            else {
                return StepStatus.DONE;
            }

            if (visitedNodes.contains(node))
                continue;

            if (analyzeNode(node, logger)) {
                visitedNodes.add(node);
                nodeQueue.addChildren(node.firstChild());
            }
            else {
                revisitQueue.addNode(node);
            }

            if (revisitQueue.hasNext() || nodeQueue.hasNext())
                return StepStatus.SUCCESS;
        }
    }


    /**
     * Analyzes a specific node in the {@link IndexBenefitGraph graph}.
     *
     * @param node
     *      the node being analyzed
     * @param logger
     *      the logger used to log interactions
     * @return
     *      whether or not the analyses completed. When the analysis doesn't complete it is due to 
     *      the IBG not being completely expanded.
     */
    private boolean analyzeNode(IndexBenefitGraph.Node node, InteractionLogger logger)
    {
        BitArraySet<Index> bitsetY = node.getConfiguration();

        // get the used set
        usedBitSet.clear();
        node.addUsedIndexes(usedBitSet);

        // store the used set
        allUsedIndexes.addAll(usedBitSet);

        // set up candidates
        candidatesBitSet.clear();
        candidatesBitSet.addAll(rootBitSet);
        candidatesBitSet.removeAll(usedBitSet);
        candidatesBitSet.retainAll(allUsedIndexes);

        // set false on first failure
        boolean retval = true;

        for (Index a : candidatesBitSet) {
            IndexBenefitGraph.Node y;
            double costY;

            // Y is just the current node
            y = node;
            costY = y.cost();

            // fetch YaSimple
            bitsetYaSimple.clear();
            bitsetYaSimple.addAll(bitsetY);
            bitsetYaSimple.add(a);

            IndexBenefitGraph.Node yaSimple =
                coveringNodeFinder.findFast(ibgCons.rootNode(), bitsetYaSimple, null);

            if (yaSimple == null)
                retval = false;
            else
                logger.assignBenefit(a, costY - yaSimple.cost());

            for (Index b : candidatesBitSet) {
                IndexBenefitGraph.Node ya;
                IndexBenefitGraph.Node yab;
                IndexBenefitGraph.Node ybPlus;
                IndexBenefitGraph.Node ybMinus;
                double costYa;
                double costYab;

                // fetch Ya and Yab
                bitsetYa.clear();
                bitsetYa.addAll(bitsetY);
                bitsetYa.add(a);
                bitsetYa.remove(b);

                bitsetYab.clear();
                bitsetYab.addAll(bitsetY);
                bitsetYab.add(a);
                bitsetYab.add(b);

                yab = coveringNodeFinder.findFast(ibgCons.rootNode(), bitsetYab, yaSimple);
                ya = coveringNodeFinder.findFast(ibgCons.rootNode(), bitsetYa, yab);

                if (ya == null) {
                    retval = false;
                    continue;
                }
                if (yab == null) {
                    retval = false;
                    continue;
                }
                costYa = ya.cost();
                costYab = yab.cost();

                // fetch YbMinus and YbPlus
                bitsetYbMinus.clear();
                y.addUsedIndexes(bitsetYbMinus);
                ya.addUsedIndexes(bitsetYbMinus);
                yab.addUsedIndexes(bitsetYbMinus);
                bitsetYbMinus.remove(a);
                bitsetYbMinus.add(b);

                bitsetYbPlus.clear();
                bitsetYbPlus.addAll(bitsetY);
                bitsetYbPlus.remove(a);
                bitsetYbPlus.add(b);

                ybPlus = coveringNodeFinder.findFast(ibgCons.rootNode(), bitsetYbPlus, yab);
                ybMinus = coveringNodeFinder.findFast(ibgCons.rootNode(), bitsetYbMinus, ybPlus);

                // try to set lower bound based on Y, Ya, YbPlus, and Yab
                if (ybPlus != null)
                    logger.assignInteraction(
                            a, b, interactionLevel(costY, costYa, ybPlus.cost(), costYab));
                else
                    retval = false;

                // try to set lower bound based on Y, Ya, YbMinus, and Yab
                if (ybMinus != null)
                    logger.assignInteraction(
                            a, b, interactionLevel(costY, costYa, ybMinus.cost(), costYab));
                else
                    retval = false;
            }
        }

        return retval;
    }

    /**
     * Compute the interaction level based on the given four costs. This corresponds to:
     *
     * <code>
     * $| C - C_a - C_b + C_ab |$
     * </code>
     *
     * @param empty
     *      the empty cost
     * @param a
     *      the cost of index a
     * @param b
     *      the cost of index b
     * @param ab
     *      the cost of index ab
     * @return
     *      the level of interaction for indexes associated with costs {@code a} and {@code b}.
     */
    private static double interactionLevel(double empty, double a, double b, double ab)
    {
        return Math.abs(empty - a - b + ab);
    }

    /**
     * Perform one step of analysis. Return true if there might be some work left to do for this 
     * analysis
     */
    public enum StepStatus
    {
        /**
         * there was no expanded node to analyze, and there is an unexpanded node.
         */
        BLOCKED,

        /**
         * all nodes are analyzed.
         */
        DONE,

        /**
         * there was an expanded node to analyze.
         */
        SUCCESS
    }
}

