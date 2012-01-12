package edu.ucsc.dbtune.advisor.interactions;

import java.sql.SQLException;

import java.util.Set;

import edu.ucsc.dbtune.advisor.interactions.InteractionBank;

import edu.ucsc.dbtune.ibg.IBGCoveringNodeFinder;
import edu.ucsc.dbtune.ibg.IBGNodeQueue;
import edu.ucsc.dbtune.ibg.IndexBenefitGraph;
import edu.ucsc.dbtune.ibg.IndexBenefitGraphConstructor;
import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.util.BitArraySet;

/**
 * This class implements the qINTERACT algorithm described in Schnaitter et. al. for computing the 
 * degree of interaction for all the pairs {@latex.inline $a,b \\in S$}.
 *
 * @author Karl Schnaitter
 * @see <a href="http://portal.acm.org/citation.cfm?id=1687766">
 *         Index interactions in physical design tuning: modeling, analysis, and applications
 *      </a>
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
    private final Set<Index> allUsedIndexes;

    // graph traversal objects
    private IBGCoveringNodeFinder coveringNodeFinder = new IBGCoveringNodeFinder();

    // copy of the root bit set for convenience
    private final Set<Index> rootBitSet;

    // keeps track of visited nodes
    private final Set<IndexBenefitGraph.Node> visitedNodes;

    private final IndexBenefitGraph.Node rootNode;

    // All the following correspond to a bunch of structures that we keep around to
    // avoid excessive garbage collection. These structures are only used in the
    // analyzeNode() method.
    private Set<Index> candidatesBitSet = new BitArraySet<Index>();
    private Set<Index> usedBitSet = new BitArraySet<Index>();
    private Set<Index> bitsetYaSimple = new BitArraySet<Index>();
    private Set<Index> bitsetYa = new BitArraySet<Index>();
    private Set<Index> bitsetYbMinus = new BitArraySet<Index>();
    private Set<Index> bitsetYbPlus = new BitArraySet<Index>();
    private Set<Index> bitsetYab = new BitArraySet<Index>();

    /**
     * construct an {@code IBGAnalyzer}.
     *
     * @param ibgCons
     *      a given {@link IndexBenefitGraphConstructor} object.
     */
    private IBGAnalyzer(IndexBenefitGraphConstructor ibgCons)
    {
        // initialize fields
        this.ibgCons = ibgCons;
        this.nodeQueue = new IBGNodeQueue();
        this.revisitQueue = new IBGNodeQueue();
        allUsedIndexes = new BitArraySet<Index>();
        rootBitSet = new BitArraySet<Index>(ibgCons.rootNode().getConfiguration());
        visitedNodes = new BitArraySet<IndexBenefitGraph.Node>();

        // seed the queue with the root node
        nodeQueue.addNode(ibgCons.rootNode());

        rootNode = ibgCons.rootNode();
    }

    /**
     * construct an {@code IBGAnalyzer}.
     *
     * @param ibgCons
     *      a given {@link IndexBenefitGraphConstructor} object.
     */
    private IBGAnalyzer(IndexBenefitGraph ibg)
    {
        // initialize fields
        ibgCons = null;
        nodeQueue = new IBGNodeQueue();
        revisitQueue = new IBGNodeQueue();
        allUsedIndexes = new BitArraySet<Index>();
        rootBitSet = new BitArraySet<Index>(ibgCons.rootNode().getConfiguration());
        visitedNodes = new BitArraySet<IndexBenefitGraph.Node>();

        // seed the queue with the root node
        nodeQueue.addNode(ibg.rootNode());

        rootNode = ibg.rootNode();
    }

    /**
     * traverses the {@link IndexBenefitGraph}.
     *
     * @param bank
     *      a logger that keeps tracks of the visited nodes.
     * @param wait
     *      a flag that indicates if {@link IndexBenefitGraphConstructor}
     *      should wait ({@code true}) for a node in the graph to expand
     *      before doing something.
     * @return either {@link StepStatus#BLOCKED}, {@link StepStatus#DONE}, or
     *      {@link StepStatus#SUCCESS}.
     */
    public final StepStatus analysisStep(InteractionBank bank, boolean wait) throws SQLException
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

                    if (ibgCons == null)
                        throw new SQLException("IBGConstructor not defined");
                    else
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

            if (analyzeNode(rootNode, node, bank)) {
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
     * @param bank
     *      the logger used to log interactions
     * @return
     *      whether or not the analyses completed. When the analysis doesn't complete it is due to 
     *      the IBG not being completely expanded.
     */
    private boolean analyzeNode(
            IndexBenefitGraph.Node root, IndexBenefitGraph.Node node, InteractionBank bank)
    {
        Set<Index> bitsetY = node.getConfiguration();

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
                coveringNodeFinder.findFast(root, bitsetYaSimple, null);

            if (yaSimple == null)
                retval = false;
            else
                bank.assignBenefit(a, costY - yaSimple.cost());

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

                yab = coveringNodeFinder.findFast(root, bitsetYab, yaSimple);
                ya = coveringNodeFinder.findFast(root, bitsetYa, yab);

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

                ybPlus = coveringNodeFinder.findFast(root, bitsetYbPlus, yab);
                ybMinus = coveringNodeFinder.findFast(root, bitsetYbMinus, ybPlus);

                // try to set lower bound based on Y, Ya, YbPlus, and Yab
                if (ybPlus != null)
                    bank.assignInteraction(
                            a, b, interactionLevel(costY, costYa, ybPlus.cost(), costYab));
                else
                    retval = false;

                // try to set lower bound based on Y, Ya, YbMinus, and Yab
                if (ybMinus != null)
                    bank.assignInteraction(
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

    /**
     * Analyze an IBG to identify index interactions. The IBG is assumed to be fully constructed.
     *
     * @param ibg
     *      the IBG to be analyzed
     * @return
     *      the interactions that were identified
     * @throws SQLException
     *      if the ibg is not fully constructed
     */
    public static InteractionBank analyze(IndexBenefitGraph ibg) throws SQLException
    {
        InteractionBank bank = new InteractionBank(ibg.rootNode().getConfiguration());
        IBGAnalyzer analyzer = new IBGAnalyzer(ibg);

        boolean done = false;

        while (!done) {
            switch (analyzer.analysisStep(bank, false)) {
                case DONE:
                    done = true;
                    break;
                case SUCCESS:
                    break;
                case BLOCKED:
                default:
                    throw new SQLException("Error in computing interactions");
            }
        }

        return bank;
    }

    private static class ThreadIBGAnalysis implements Runnable
    {
        private IBGAnalyzer analyzer = null;
        private InteractionBank bank = null;

        private final Object taskMonitor = new Object();
        private State state = State.IDLE;

        private enum State
        { IDLE, PENDING, DONE };

        @Override
        public void run()
        {
            while (true) {
                synchronized (taskMonitor) {
                    while (state != State.PENDING) {
                        try {
                            taskMonitor.wait();
                        } catch (InterruptedException e) {
                            throw new RuntimeException(e);
                        }
                    }
                }

                boolean done = false;

                try {
                while (!done) {
                    switch (analyzer.analysisStep(bank, true)) {
                        case SUCCESS:
                            //if (++analyzedCount % 1000 == 0) Debug.println("a" + analyzedCount);
                            break;
                        case DONE:
                            done = true;
                            break;
                        case BLOCKED:
                            //Debug.logError("unexpected BLOCKED result from analysisStep");
                            return;
                        default:
                            //Debug.logError("unexpected result from analysisStep");
                            return;
                    }
                }
                } catch(SQLException e) {
                    throw new RuntimeException(e);
                }

                synchronized (taskMonitor) {
                    state = State.DONE;
                    taskMonitor.notify();
                }
            }
        }

        /*
         * tell the analysis thread to start analyzing, and return immediately
         */
        public void startAnalysis(IBGAnalyzer analyzer0, InteractionBank bank0)
        {
            synchronized (taskMonitor) {
                if (state == State.PENDING) {
                    throw new RuntimeException("unexpected state in IBG startAnalysis");
                }

                analyzer = analyzer0;
                bank = bank0;
                state = State.PENDING;
                taskMonitor.notify();
            }
        }

        public void waitUntilDone()
        {
            synchronized (taskMonitor) {
                while (state == State.PENDING) {
                    try {
                        taskMonitor.wait();
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }

                analyzer = null;
                bank = null;
                state = State.IDLE;
            }
        }
    }
}
