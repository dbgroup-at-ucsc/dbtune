package edu.ucsc.dbtune.ibg;

import edu.ucsc.dbtune.advisor.interactions.InteractionLogger;
import edu.ucsc.dbtune.ibg.IndexBenefitGraph.IBGNode;
import edu.ucsc.dbtune.util.IndexBitSet;

public class IBGAnalyzer {
    // the IBG we are currently exploring
    protected final IndexBenefitGraphConstructor ibgCons;

    // queue of nodes to explore
    private final IBGNodeQueue nodeQueue;

    // queue of nodes to revisit
    private final IBGNodeQueue revisitQueue;

    // the set of all used indexes seen so far in the IBG
    // this is different from the bank, because that may have
    // used indexes from other IBG traversals
    private final IndexBitSet allUsedIndexes;

    // graph traversal objects
    private IBGCoveringNodeFinder coveringNodeFinder = new IBGCoveringNodeFinder();

    // copy of the root bit set for convenience
    private final IndexBitSet rootBitSet;

    // keeps track of visited nodes
    private final IndexBitSet visitedNodes;

    /**
     * construct an {@code IBGAnalyzer}.
     * @param ibgCons
     *      a given {@link IndexBenefitGraphConstructor} object.
     * @param nodeQueue
     *      a given {@link IBGNodeQueue} object which contains IBG nodes.
     * @param revisitQueue
     *      a given {@link IBGNodeQueue} object which contains IBG node that will be revisited.
     */
    public IBGAnalyzer(IndexBenefitGraphConstructor ibgCons, IBGNodeQueue nodeQueue, IBGNodeQueue revisitQueue){
        // initialize fields
        this.ibgCons        = ibgCons;
        this.nodeQueue      = nodeQueue;
        this.revisitQueue   = revisitQueue;
        allUsedIndexes      = new IndexBitSet();
        rootBitSet          = ibgCons.rootNode().getConfiguration().clone();
        visitedNodes        = new IndexBitSet();

        // seed the queue with the root node
        nodeQueue.addNode(ibgCons.rootNode());
    }

    /**
     * construct an {@code IBGAnalyzer}.
     * @param ibgCons
     *      a given {@link IndexBenefitGraphConstructor} object.
     */
    public IBGAnalyzer(IndexBenefitGraphConstructor ibgCons) {
        this(ibgCons, new IBGNodeQueue(), new IBGNodeQueue());
    }

    /**
     * traverses the {@link IndexBenefitGraph}.
     * @param logger
     *      a logger that keeps tracks of the visited nodes.
     * @param wait
     *      a flag that indicates if {@link IndexBenefitGraphConstructor}
     *      should wait ({@code true}) for a node in the graph to expand
     *      before doing something.
     * @return either {@link StepStatus#BLOCKED}, {@link StepStatus#DONE}, or
     *      {@link StepStatus#SUCCESS}.
     */
    public final StepStatus analysisStep(InteractionLogger logger, boolean wait) {
        // we might need to go through several nodes to find one that we haven't visited yet
        while (true) {
            IBGNode node;

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

            if (visitedNodes.get(node.getID()))
                continue;

            if (analyzeNode(node, logger)) {
                visitedNodes.set(node.getID());
                nodeQueue.addChildren(node.firstChild());
            }
            else {
                revisitQueue.addNode(node);
            }

            if (revisitQueue.hasNext() || nodeQueue.hasNext())
                return StepStatus.SUCCESS;
        }
    }


    /*
     * analysis a specific node in the {@link IndexBenefitGraph graph}.
     * @param node
     *      an {@link IBGNode node} in the graph.
     * @param logger
     *      an {@link InteractionLogger graph logger}.
     * @return {@code true} if the analysis was successful.
     */
    // We have a bunch of structures that we keep around to
    // avoid excessive garbage collection. These structures are only used in
    // analyzeNode().
    //
    private IndexBitSet candidatesBitSet = new IndexBitSet();
    private IndexBitSet usedBitSet = new IndexBitSet();

    private IndexBitSet bitset_YaSimple = new IndexBitSet();
    private IndexBitSet bitset_Ya = new IndexBitSet();
    private IndexBitSet bitset_YbMinus = new IndexBitSet();
    private IndexBitSet bitset_YbPlus = new IndexBitSet();
    private IndexBitSet bitset_Yab = new IndexBitSet();

    /*
     * Return true if the analysis was successful
     */
    private boolean analyzeNode(IBGNode node, InteractionLogger logger) {
        IndexBitSet bitset_Y = node.getConfiguration();

        // get the used set
        usedBitSet.clear();
        node.addUsedIndexes(usedBitSet);

        // store the used set
        allUsedIndexes.or(usedBitSet);

        // set up candidates
        candidatesBitSet.set(rootBitSet);
        candidatesBitSet.andNot(usedBitSet);
        candidatesBitSet.and(allUsedIndexes);

        boolean retval = true; // set false on first failure
        for (int a = candidatesBitSet.nextSetBit(0); a >= 0; a = candidatesBitSet.nextSetBit(a+1)) {
            IBGNode Y;
            double costY;

            // Y is just the current node
            Y = node;
            costY = Y.cost();

            // fetch YaSimple
            bitset_YaSimple.set(bitset_Y);
            bitset_YaSimple.set(a);
            IBGNode YaSimple = coveringNodeFinder.findFast(ibgCons.rootNode(), bitset_YaSimple, null);
            if (YaSimple == null)
                retval = false;
            else
                logger.assignBenefit(a, costY - YaSimple.cost());

            for (int b = candidatesBitSet.nextSetBit(a+1); b >= 0; b = candidatesBitSet.nextSetBit(b+1)) {
                IBGNode Ya, Yab, YbPlus, YbMinus;
                double costYa, costYab;

                // fetch Ya and Yab
                bitset_Ya.set(bitset_Y);
                bitset_Ya.set(a);
                bitset_Ya.clear(b);

                bitset_Yab.set(bitset_Y);
                bitset_Yab.set(a);
                bitset_Yab.set(b);

                Yab = coveringNodeFinder.findFast(ibgCons.rootNode(), bitset_Yab, YaSimple);
                Ya = coveringNodeFinder.findFast(ibgCons.rootNode(), bitset_Ya, Yab);

                if (Ya == null) {
                    retval = false;
                    continue;
                }
                if (Yab == null) {
                    retval = false;
                    continue;
                }
                costYa = Ya.cost();
                costYab = Yab.cost();

                // fetch YbMinus and YbPlus
                bitset_YbMinus.clear();
                Y.addUsedIndexes(bitset_YbMinus);
                Ya.addUsedIndexes(bitset_YbMinus);
                Yab.addUsedIndexes(bitset_YbMinus);
                bitset_YbMinus.clear(a);
                bitset_YbMinus.set(b);

                bitset_YbPlus.set(bitset_Y);
                bitset_YbPlus.clear(a);
                bitset_YbPlus.set(b);

                YbPlus = coveringNodeFinder.findFast(ibgCons.rootNode(), bitset_YbPlus, Yab);
                YbMinus = coveringNodeFinder.findFast(ibgCons.rootNode(), bitset_YbMinus, YbPlus);

                // try to set lower bound based on Y, Ya, YbPlus, and Yab
                if (YbPlus != null) {
                    logger.assignInteraction(a, b, interactionLevel(costY, costYa, YbPlus.cost(), costYab));
                }
                else {
                    retval = false;
                }

                // try to set lower bound based on Y, Ya, YbMinus, and Yab
                if (YbMinus != null) {
                    logger.assignInteraction(a, b, interactionLevel(costY, costYa, YbMinus.cost(), costYab));
                }
                else {
                    retval = false;
                }
            }
        }

        return retval;
    }


    /*
     * Compute the interaction level based on the four costs
     *
     *     | C - C_a - C_b + C_ab |
     */
    private static double interactionLevel(double empty, double a, double b, double ab) {
        return Math.abs(empty - a - b + ab);
    }


    /**
     * Perform one step of analysis
     * 
     * Return true if there might be some work left to do for this analysis
     */
    public enum StepStatus {
        /**
         * there was no expanded node to analyze, and there is an unexpanded node
         */
        BLOCKED,

        /**
         * all nodes are analyzed
         */
        DONE,

        /**
         * there was an expanded node to analyze
         */
        SUCCESS
    }
}

