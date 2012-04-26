package edu.ucsc.dbtune.ibg;

import java.sql.SQLException;
import java.util.HashSet;
import java.util.Set;

import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.optimizer.ExplainedSQLStatement;
import edu.ucsc.dbtune.optimizer.Optimizer;
import edu.ucsc.dbtune.util.BitArraySet;
import edu.ucsc.dbtune.util.DefaultQueue;
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
    /* 
     * parameters of the construction.
     */
    protected SQLStatement sql;
    protected Set<Index> configuration;
    protected Optimizer optimizer;

    /* Counter for assigning unique node IDs */
    private int nodeCount;

    /* temporary bit sets allocated once to allow reuse... only used in buildNode() */ 
    private Set<Index> used;
    private BitArraySet<Index> children;

    /* Every node in the graph is a descendant of rootNode */
    private IndexBenefitGraph.Node rootNode;

    /* the queue of pending nodes to expand */
    private DefaultQueue<IndexBenefitGraph.Node> queue;

    /* a monitor for waiting on a node expansion */
    private final Object nodeExpansionMonitor = new Object();

    /* an object that allows for covering node searches */
    private final IBGCoveringNodeFinder coveringNodeFinder = new IBGCoveringNodeFinder();

    /**
     * Creates an IBG constructor.
     */
    private IndexBenefitGraphConstructor()
    {
    }

    /**
     * @return the {@link Node root node}.
     */
    public IndexBenefitGraph.Node rootNode()
    {
        return rootNode;
    }

    /**
     * Wait for a specific node to be expanded.
     *
     * @param node
     *      a node to be expanded.
     */
    public void waitUntilExpanded(IndexBenefitGraph.Node node)
    {
        synchronized (nodeExpansionMonitor) {
            while (!node.isExpanded())
                try {
                    nodeExpansionMonitor.wait();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
        }
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
        IndexBenefitGraph.Node newNode;
        IndexBenefitGraph.Node coveringNode;
        ExplainedSQLStatement stmt;
        double totalCost;

        if (queue.isEmpty())
            return false;

        newNode = queue.remove();

        // get cost and used set (stored into used)
        used.clear();
        coveringNode = coveringNodeFinder.find(rootNode, newNode.getConfiguration());
        if (coveringNode != null) {
            totalCost = coveringNode.cost();
            coveringNode.addUsedIndexes(used);
            used = new HashSet<Index>();
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
        IndexBenefitGraph.Node.Child firstChild = null;
        IndexBenefitGraph.Node.Child lastChild = null;
        children.clear();
        children.addAll(newNode.getConfiguration());

        for (Index u : used) {

            children.remove(u);
            IndexBenefitGraph.Node childNode = find(queue, children);

            if (childNode == null) {
                childNode =
                    new IndexBenefitGraph.Node(new BitArraySet<Index>(children), nodeCount++);
                queue.add(childNode);
            }

            children.add(u);

            IndexBenefitGraph.Node.Child child = new IndexBenefitGraph.Node.Child(childNode, u);

            if (firstChild == null)
                firstChild = child;
            else
                lastChild.setNext(child);

            lastChild = child;
        }

        // Expand the node and notify waiting threads
        synchronized (nodeExpansionMonitor) {
            newNode.expand(totalCost, firstChild);
            nodeExpansionMonitor.notifyAll();
        }

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
            DefaultQueue<IndexBenefitGraph.Node> queue, Set<Index> configuration)
    {
        for (int i = 0; i < queue.count(); i++) {
            IndexBenefitGraph.Node node = queue.fetch(i);

            if (node.getConfiguration().equals(configuration))
                return node;
        }

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
        this.configuration = conf;
        this.nodeCount = 0;
        this.used = new BitArraySet<Index>();
        this.children = new BitArraySet<Index>();
        this.queue = new DefaultQueue<IndexBenefitGraph.Node>();
        this.rootNode = new IndexBenefitGraph.Node(conf, nodeCount++);

        queue.add(rootNode);

        ThreadIBGConstruction ibgConstruction = new ThreadIBGConstruction();
        Thread ibgContructionThread = new Thread(ibgConstruction);
        ibgContructionThread.setName("IBG Construction");
        ibgContructionThread.start();

        ibgConstruction.startConstruction(this);
        ibgConstruction.waitUntilDone();

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

    //CHECKSTYLE:OFF
    private static class ThreadIBGConstruction implements Runnable
    {
        private IndexBenefitGraphConstructor ibgCons = null;

        private Object taskMonitor = new Object();
        private State state = State.IDLE;

        private enum State
        { IDLE, PENDING, DONE };

        public ThreadIBGConstruction()
        {
        }

        public void run()
        {
            while (true) {
                synchronized (taskMonitor) {
                    while (state != State.PENDING) {
                        try {
                            taskMonitor.wait();
                        } catch (InterruptedException e) { }
                    }
                }

                try {
                    boolean success;
                    do {
                        success = ibgCons.buildNode();
                    } while (success);
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }

                synchronized (taskMonitor) {
                    state = State.DONE;
                    taskMonitor.notify();
                }
            }
        }

        /*
         * tells the construction thread to start constructing an IBG, and returns immediately
         */
        public void startConstruction(IndexBenefitGraphConstructor ibgCons0)
        {
            synchronized (taskMonitor) {
                if (state == State.PENDING) {
                    throw new RuntimeException("unexpected state in IBG startConstruction");
                }

                ibgCons = ibgCons0;
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
                    } catch (InterruptedException e) { }
                }

                ibgCons = null;
                state = State.IDLE;
            }
        }
    }
    //CHECKSTYLE:ON
}
