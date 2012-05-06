package edu.ucsc.dbtune.advisor.interactions;

import java.sql.SQLException;

import java.util.Deque;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Set;

import edu.ucsc.dbtune.ibg.IBGCoveringNodeFinder;
import edu.ucsc.dbtune.ibg.IndexBenefitGraph;
import edu.ucsc.dbtune.metadata.Index;

/**
 * This class implements the qINTERACT algorithm described in Schnaitter et. al. for computing the 
 * degree of interaction for all the pairs {@latex.inline $a,b \\in S$}.
 *
 * @author Karl Schnaitter
 * @author Ivo Jimenez
 * @see <a href="http://portal.acm.org/citation.cfm?id=1687766">
 *         Index interactions in physical design tuning: modeling, analysis, and applications
 *      </a>
 */
public final class IBGAnalyzer
{
    // queue of nodes to explore
    private final Deque<IndexBenefitGraph.Node> nodeQueue;

    // queue of nodes to revisit
    private final Deque<IndexBenefitGraph.Node> revisitQueue;

    // the set of all used indexes seen so far in the IBG
    // this is different from the bank, because that may have
    // used indexes from other IBG traversals
    private final Set<Index> allUsedIndexes;

    // graph traversal objects
    private IBGCoveringNodeFinder coveringNodeFinder = new IBGCoveringNodeFinder();

    // keeps track of visited nodes
    private final Set<IndexBenefitGraph.Node> visitedNodes;

    // the node of the IBG we are currently exploring
    private final IndexBenefitGraph.Node rootNode;

    /**
     * construct an {@code IBGAnalyzer}.
     *
     * @param ibg
     *      ibg to be analyzed
     */
    private IBGAnalyzer(IndexBenefitGraph ibg)
    {
        // initialize fields 
        nodeQueue = new LinkedList<IndexBenefitGraph.Node>();
        revisitQueue = new LinkedList<IndexBenefitGraph.Node>();
        allUsedIndexes = new HashSet<Index>();
        visitedNodes = new HashSet<IndexBenefitGraph.Node>();
        
        rootNode = ibg.rootNode();

        // seed the queue with the root node
        nodeQueue.add(rootNode);
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
     * @return
     *      either {@link StepStatus#BLOCKED}, {@link StepStatus#DONE}, or
     *      {@link StepStatus#SUCCESS}.
     * @throws SQLException
     *      if an error occurs
     */
    public StepStatus analysisStep(InteractionBank bank, boolean wait) throws SQLException
    {
        // we might need to go through several nodes to find one that we haven't visited yet
        while (true) {
            IndexBenefitGraph.Node node;

            if (!nodeQueue.isEmpty()) {
                if (nodeQueue.peek().isExpanded()) {
                    node = nodeQueue.pop();
                }
                else if (wait) {
                    node = nodeQueue.pop();

                    throw new SQLException("IBGConstructor not defined");
                }
                else if (!revisitQueue.isEmpty()) {
                    node = revisitQueue.pop();
                }
                else {
                    return StepStatus.BLOCKED;
                }
            }
            else if (!revisitQueue.isEmpty()) {
                node = revisitQueue.pop();
            }
            else {
                return StepStatus.DONE;
            }

            if (visitedNodes.contains(node))
                continue;

            if (analyzeNode(node, bank)) {
                visitedNodes.add(node);
                nodeQueue.addAll(node.getChildren());
            }
            else {
                revisitQueue.add(node);
            }

            if (!revisitQueue.isEmpty() || !nodeQueue.isEmpty())
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
    private boolean analyzeNode(IndexBenefitGraph.Node node, InteractionBank bank)
    {
        Set<Index> candidates = new HashSet<Index>();
        Set<Index> used = new HashSet<Index>();
        Set<Index> bitsetYaSimple = new HashSet<Index>();
        Set<Index> bitsetYa = new HashSet<Index>();
        Set<Index> bitsetYbMinus = new HashSet<Index>();
        Set<Index> bitsetYbPlus = new HashSet<Index>();
        Set<Index> bitsetYab = new HashSet<Index>();
        Set<Index> bitsetY = node.getConfiguration();

        // get the used set
        used.addAll(node.getUsedIndexes());

        // store the used set
        allUsedIndexes.addAll(used);

        // set up candidates 
        candidates.addAll(rootNode.getConfiguration());
        candidates.removeAll(used);
        candidates.retainAll(allUsedIndexes);

        // set false on first failure
        boolean retval = true;

        for (Index a : candidates) {
            IndexBenefitGraph.Node y;
            double costY;

            // Y is just the current node
            y = node;
            costY = y.cost();

            // fetch YaSimple
            bitsetYaSimple.addAll(bitsetY);
            bitsetYaSimple.add(a);

            IndexBenefitGraph.Node yaSimple = coveringNodeFinder.find(rootNode, bitsetYaSimple);

            if (yaSimple == null)
                retval = false;
            else
                bank.assignBenefit(a, costY - yaSimple.cost());

            for (Index b : candidates) {
                if (a.equals(b))
                    continue;
                IndexBenefitGraph.Node ya;
                IndexBenefitGraph.Node yab;
                IndexBenefitGraph.Node ybPlus;
                IndexBenefitGraph.Node ybMinus;
                double costYa;
                double costYab;

                // if (bank.interactionExists(a,b))
                //     // XXX: try to see how strong the interaction can get?? This might not affect
                //     // performance a lot
                //     continue;

                // fetch Ya and Yab
                bitsetYa.addAll(bitsetY);
                bitsetYa.add(a);
                bitsetYa.remove(b);

                bitsetYab.addAll(bitsetY);
                bitsetYab.add(a);
                bitsetYab.add(b);

                ya = coveringNodeFinder.find(rootNode, bitsetYa);
                yab = coveringNodeFinder.find(rootNode, bitsetYab);

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
                bitsetYbMinus.addAll(y.getUsedIndexes());
                bitsetYbMinus.addAll(ya.getUsedIndexes());
                bitsetYbMinus.addAll(yab.getUsedIndexes());
                bitsetYbMinus.remove(a);
                bitsetYbMinus.add(b);

                bitsetYbPlus.addAll(bitsetY);
                bitsetYbPlus.remove(a);
                bitsetYbPlus.add(b);

                ybMinus = coveringNodeFinder.find(rootNode, bitsetYbMinus);
                ybPlus = coveringNodeFinder.find(rootNode, bitsetYbPlus);

                // try to set lower bound based on Y, Ya, YbPlus, and Yab
                if (ybPlus != null)
                    bank.assignInteraction(
                            a, b, interactionLevel(costY, costYa, ybPlus.cost(), costYab));
                else
                    retval = false;

                // try to set lower bound based on Y, Ya, YbMinus, and Yab
                if (ybMinus != null)
                    bank.assignInteraction(a, b, interactionLevel(costY, costYa, ybMinus.cost(), 
                                costYab));
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
}
