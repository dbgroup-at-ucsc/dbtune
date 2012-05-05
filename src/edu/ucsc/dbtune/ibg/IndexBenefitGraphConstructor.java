package edu.ucsc.dbtune.ibg;

import java.sql.SQLException;
import java.util.Deque;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Set;

import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.optimizer.ExplainedSQLStatement;
import edu.ucsc.dbtune.optimizer.Optimizer;
import edu.ucsc.dbtune.workload.SQLStatement;

/**
 * An IBG is naturally constructed by a top-down process, starting from {@latex.inline $S$} as the 
 * topmost node. For each node {@latex.inline $Y$} in the IBG, the process performs a what-if 
 * optimization and, for each {@latex.inline $a \\in used_q(Y)$}, adds {@latex.inline $Y − {a}$} to 
 * the children of {@latex.inline $Y$}. Each child is built recursively unless it already exists, 
 * which may be checked by storing nodes in a hash table. Overall, constructing an IBG with 
 * {@latex.inline $N$} nodes and fan-out {@latex.inline $f$} requires {@latex.inline $N$} what-if 
 * optimizations, {@latex.inline $O(fN)$} operations on the hash table of index-sets, and 
 * {@latex.inline $O(fN)$} other basic operations.
 * <p>
 * The key property of the IBG is that it is sufficient to derive {@latex.inline $cost_q(X)$} and 
 * {@latex.inline $used_q(X)$} for any index-set {@latex.inline $X \\subseteq S$}, even if 
 * {@latex.inline $X$} is not represented directly in the IBG.
 *
 * @author Karl Schnaitter
 * @author Huascar Sanchez
 * @author Ivo Jimenez
 * @author Neoklis Polyzotis
 *
 * @see <a href="http://portal.acm.org/citation.cfm?id=1687766">
 *     Index interactions in physical design tuning: modeling, analysis, and applications</a>
 */
public final class IndexBenefitGraphConstructor
{
    /**
     * parameters of the construction.
     */
    protected SQLStatement sql;
    protected Set<Index> configuration;
    protected Optimizer optimizer;

    /* Counter for assigning unique node IDs */
    private int nodeCount;

    /* Every node in the graph is a descendant of rootNode */
    private IndexBenefitGraph.Node rootNode;

    /* the queue of pending nodes to expand */
    private Deque<IndexBenefitGraph.Node> queue;

    /* an object that allows for covering node searches */
    private final IBGCoveringNodeFinder coveringNodeFinder = new IBGCoveringNodeFinder();

    /**
     * Creates an IBG constructor.
     */
    private IndexBenefitGraphConstructor()
    {
    }

    /**
     * Expands one node of the IBG. Returns true if a node was expanded, or false if there are no 
     * unexpanded nodes.
     * <p>
     * This method is not safe to be called from more than one thread.
     *
     * @return
     *      if the node built successfully
     * @throws SQLException
     *      if the what-if optimization call executed by the {@link Optimizer} fails
     */
    private boolean buildNode() throws SQLException
    {
        Set<Index> used;
        IndexBenefitGraph.Node newNode;
        IndexBenefitGraph.Node coveringNode;
        ExplainedSQLStatement stmt;
        double totalCost;

        if (queue.isEmpty())
            return false;

        newNode = queue.remove();

        // get cost and used set (stored into used)
        coveringNode = coveringNodeFinder.find(rootNode, newNode.getConfiguration());
        if (coveringNode != null) {
            totalCost = coveringNode.cost();
            used = new HashSet<Index>();
            used.addAll(coveringNode.getUsedIndexes());
        } else {
            stmt = optimizer.explain(sql, newNode.getConfiguration());
            totalCost = stmt.getSelectCost();
            used = stmt.getUsedConfiguration();
        }

        // create the child list
        // if any Node did not exist yet, add it to the queue
        // We make sure to keep the child list in the same order as the nodeQueue, so that
        // analysis and construction can move in lock step. This is done by keeping both
        // in order of construction.
        Set<Index> children = new HashSet<Index>();
        children.addAll(newNode.getConfiguration());

        for (Index u : used) {

            if (!children.remove(u))
                throw new RuntimeException("Couldn't remove index " + u + " from children set"); 

            IndexBenefitGraph.Node childNode = find(queue, children);

            if (childNode == null) {
                childNode = new IndexBenefitGraph.Node(new HashSet<Index>(children), nodeCount++);
                queue.add(childNode);
            }

            if (!children.add(u))
                throw new RuntimeException("Couldn't add index " + u + " to children set"); 

            newNode.addChild(childNode, u);
        }

        newNode.setCost(totalCost);

        return !queue.isEmpty();
    }

    /**
     * Auxiliary method for {@link buildNode}. Iterates through the queue in order to find a node 
     * that corresponds to the given configuration.
     *
     * @param queue
     *      a queue of graph nodes
     * @param configuration
     *      the configuration
     * @return
     *      a node corresponding to the given configuration; {@code null} if not found.
     */
    private static IndexBenefitGraph.Node find(
            Deque<IndexBenefitGraph.Node> queue, Set<Index> configuration)
    {
        for (IndexBenefitGraph.Node node : queue)
            if (node.getConfiguration().equals(configuration))
                return node;

        return null;
    }

    /**
     * Initializes the IBG construction process. Specifically, the rootNode is physically 
     * constructed (by obtaining the cost of the given statement under the empty configuration), but 
     * it is not expanded, so its {@code cost} and {@code usedSet} properties have not been 
     * determined.
     *
     * @param delegate
     *     used to make what-if optimization calls
     * @param sql
     *      statement being explained
     * @param emptyCost
     *      select cost of statement without any indexes
     * @param conf
     *      configuration to take into account
     * @return
     *      the newly constructed IBG
     * @throws SQLException
     *      if the what-if optimization call executed by the {@link Optimizer} fails
     */
    private IndexBenefitGraph constructIBG(
            Optimizer delegate, SQLStatement sql, double emptyCost, Set<Index> conf)
        throws SQLException
    {
        this.sql = sql;
        this.optimizer = delegate;
        this.configuration = new HashSet<Index>(conf);
        this.nodeCount = 0;
        this.queue = new LinkedList<IndexBenefitGraph.Node>();
        this.rootNode = new IndexBenefitGraph.Node(conf, nodeCount++);

        this.queue.add(rootNode);

        boolean hasMoreNodesToExpand = true;

        while (hasMoreNodesToExpand)
            hasMoreNodesToExpand = buildNode();

        IndexBenefitGraph ibg = new IndexBenefitGraph(rootNode, emptyCost);

        return ibg;
    }

    /**
     * Construct an IBG from the given parameters.
     *
     * @param delegate
     *     used to make what-if optimization calls
     * @param sql
     *     statement being explained
     * @param emptyCost
     *      select cost of statement without any indexes
     * @param conf
     *     configuration to take into account
     * @return
     *      the newly constructed IBG
     * @throws SQLException
     *      if the what-if optimization call executed by the {@link Optimizer} fails
     */
    public static IndexBenefitGraph construct(
            Optimizer delegate, SQLStatement sql, double emptyCost, Set<Index> conf)
        throws SQLException
    {
        return (new IndexBenefitGraphConstructor()).constructIBG(delegate, sql, emptyCost, conf);
    }
}
