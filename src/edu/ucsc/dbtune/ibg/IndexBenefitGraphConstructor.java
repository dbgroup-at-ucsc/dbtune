package edu.ucsc.dbtune.ibg;

import java.sql.SQLException;
import java.util.HashSet;
import java.util.Set;

import edu.ucsc.dbtune.advisor.interactions.InteractionLogger;
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
 *
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
     * Parameters of the construction.
     */
    protected SQLStatement sql;
    protected Set<Index> configuration;
    protected Optimizer optimizer;
    protected CandidatePool.Snapshot candidateSet;

    /* Counter for assigning unique node IDs */
    private int nodeCount;

    /* Temporary bit sets allocated once to allow reuse... only used in buildNode() */ 
    private BitArraySet<Index> usedBitSet;
    private BitArraySet<Index> childBitSet;

    /*
     * The primary information stored by the graph
     * 
     * Every node in the graph is a descendant of rootNode. We also keep the
     * cost of the workload under the empty configuration, stored in emptyCost.
     */
    private IndexBenefitGraph.Node rootNode;

    /* cost without indexes */
    private double emptyCost;

    /* The queue of pending nodes to expand */
    private DefaultQueue<IndexBenefitGraph.Node> queue;

    /* A monitor for waiting on a node expansion */
    private final Object nodeExpansionMonitor = new Object();

    /* An object that allows for covering node searches */
    private final IBGCoveringNodeFinder coveringNodeFinder = new IBGCoveringNodeFinder();

    /* true if the index is used somewhere in the graph */
    private BitArraySet<Index> isUsed = new BitArraySet<Index>();

    /**
     * Creates an IBG constructor.
     */
    private IndexBenefitGraphConstructor()
    {
    }

    /**
     * @return the {@link Node root node}.
     */
    IndexBenefitGraph.Node rootNode()
    {
        return rootNode;
    }

    /**
     * Wait for a specific node to be expanded.
     *
     * @param node
     *      a node to be expanded.
     */
    void waitUntilExpanded(IndexBenefitGraph.Node node)
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
     *
     * This function is not safe to be called from more than one thread.
     *
     * @return
     *      if the node built successfully
     * @throws SQLException
     *      if the what-if optimization call executed by the {@link Optimizer} fails
     */
    boolean buildNode() throws SQLException
    {
        IndexBenefitGraph.Node newNode;
        IndexBenefitGraph.Node coveringNode;
        ExplainedSQLStatement stmt;
        double totalCost;

        if (queue.isEmpty())
            return false;

        newNode = queue.remove();

        // get cost and used set (stored into usedBitSet)
        usedBitSet.clear();
        coveringNode = coveringNodeFinder.find(rootNode, newNode.getConfiguration());
        if (coveringNode != null) {
            totalCost = coveringNode.cost();
            coveringNode.addUsedIndexes(usedBitSet);
        } else {
            stmt = optimizer.explain(sql, newNode.getConfiguration());
            totalCost = stmt.getTotalCost();

            for (Index idx : configuration) {
                if (stmt.isUsed(idx)) {
                    usedBitSet.add(idx);
                }
            }
        }

        // create the child list
        // if any Node did not exist yet, add it to the queue
        // We make sure to keep the child list in the same order as the nodeQueue, so that
        // analysis and construction can move in lock step. This is done by keeping both
        // in order of construction.
        IndexBenefitGraph.Node.Child firstChild = null;
        IndexBenefitGraph.Node.Child lastChild = null;
        childBitSet.clear();
        childBitSet.addAll(newNode.getConfiguration());

        for (Index u : usedBitSet) {

            childBitSet.remove(u);
            IndexBenefitGraph.Node childNode = find(queue, childBitSet);

            if (childNode == null) {
                isUsed.add(u);
                childNode =
                    new IndexBenefitGraph.Node(new BitArraySet<Index>(childBitSet), nodeCount++);
                queue.add(childNode);
            }
            childBitSet.add(u);

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
     * Auxiliary method for {@link buildNode}. Finds a node that corresponds to the given 
     * configuration.
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
     * @param conf
     *      configuration to take into account
     * @return
     *      the newly constructed IBG
     * @throws SQLException
     *      if the what-if optimization call executed by the {@link Optimizer} fails
     */
    private IndexBenefitGraph constructIBG(Optimizer delegate, SQLStatement sql, Set<Index> conf)
        throws SQLException
    {
        BitArraySet<Index> rootConfig;
        CandidatePool pool = new CandidatePool(conf);

        this.optimizer = delegate;
        this.sql = sql;
        this.configuration = conf;
        configuration = conf;
        nodeCount = 0;
        usedBitSet = new BitArraySet<Index>();
        childBitSet = new BitArraySet<Index>();
        candidateSet = pool.getSnapshot();
        rootConfig = new BitArraySet<Index>(candidateSet.set());
        queue = new DefaultQueue<IndexBenefitGraph.Node>();
        rootNode = new IndexBenefitGraph.Node(rootConfig, nodeCount++);
        emptyCost = optimizer.explain(sql).getSelectCost();

        isUsed.clear();
        // initialize the queue
        queue.add(rootNode);

        ThreadIBGAnalysis ibgAnalysis = new ThreadIBGAnalysis();
        // note: hide the interaction bank in here, something like:
        //
        //   new ThreadIBGAnalysis(configuration);
        //
        // which internally will create an InteractionLogger. At the end, we'll pass the ibgAnalysis 
        // to the IBG constructor (since it will have the interaction bank in it)

        Thread ibgAnalysisThread = new Thread(ibgAnalysis);
        ibgAnalysisThread.setName("IBG Analysis");
        ibgAnalysisThread.start();
        ThreadIBGConstruction ibgConstruction = new ThreadIBGConstruction();
        Thread ibgContructionThread = new Thread(ibgConstruction);
        ibgContructionThread.setName("IBG Construction");
        ibgContructionThread.start();

        InteractionLogger logger = new InteractionLogger(configuration);

        IBGAnalyzer ibgAnalyzer = new IBGAnalyzer(this);

        ibgConstruction.startConstruction(this);
        ibgConstruction.waitUntilDone();
        ibgAnalysis.startAnalysis(ibgAnalyzer, logger);
        ibgAnalysis.waitUntilDone();

        IndexBenefitGraph ibg = new IndexBenefitGraph(rootNode, emptyCost, isUsed);

        return ibg;
    }

    /**
     * Construct an IBG from the given parameters.
     *
     * @param delegate
     *     used to make what-if optimization calls
     * @param sql
     *     statement being explained
     * @param conf
     *     configuration to take into account
     * @return
     *      the newly constructed IBG
     * @throws SQLException
     *      if the what-if optimization call executed by the {@link Optimizer} fails
     */
    public static IndexBenefitGraph construct(Optimizer delegate, SQLStatement sql, Set<Index> conf)
        throws SQLException
    {
        return (new IndexBenefitGraphConstructor()).constructIBG(delegate, sql, conf);
    }

    //CHECKSTYLE:OFF
    public static class CandidatePool
    {
        /* serializable fields */
        Node firstNode;
        Set<Index> configuration;
        HashSet<Index> indexSet;
        int size;

        public CandidatePool(Set<Index> conf)
        {
            firstNode = null;
            indexSet = new HashSet<Index>();
            size = -1;
            configuration = conf;
        }

        public final void addIndex(Index index) throws SQLException
        {
            if (!indexSet.contains(index)) {
                ++size;

                firstNode = new Node(index, firstNode);
                indexSet.add(index);
            }
        }

        public void addIndexes(Iterable<Index> newIndexes) throws SQLException
        {
            for (Index index : newIndexes)
                addIndex(index);
        }

        public final boolean isEmpty()
        {
            return firstNode == null;
        }

        public final boolean contains(Index index)
        {
            return indexSet.contains(index);
        }

        public Snapshot getSnapshot()
        {
            return new Snapshot(configuration, firstNode);
        }

        public java.util.Iterator<Index> iterator()
        {
            return new Iterator<Index>(firstNode);
        }

        private class Node
        {
            Index index;
            Node next;

            Node(Index index0, Node next0) {
                index = index0;
                next = next0;
            }
        }

        /*
         * A snapshot of the candidate set (immutable set of indexes)
         */
        public static class Snapshot implements Iterable<Index>
        {
            /* serializable fields */
            int maxId;
            Node first;
            BitArraySet<Index> bs;

            protected Snapshot()
            {
            }

            private Snapshot(Set<Index> conf, Node first0)
            {
                bs = new BitArraySet<Index>(conf);
            }

            public java.util.Iterator<Index> iterator()
            {
                return new Iterator<Index>(first);
            }

            public int size()
            {
                return maxId;
            }

            public Set<Index> set()
            {
                return bs; // no need to clone -- this set is immutable
            }
        }

        /*
         * Iterator for a snapshot of the candidate set
         */
        private static class Iterator<I> implements java.util.Iterator<Index>
        {
            Node next;

            Iterator(Node start) {
                next = start;
            }

            public boolean hasNext()
            {
                return next != null;
            }

            public Index next()
            {
                if (next == null)
                    throw new java.util.NoSuchElementException();
                Index current = next.index;
                next = next.next;
                return current;
            }

            public void remove()
            {
                throw new UnsupportedOperationException();
            }
        }
    }

    public static class ThreadIBGAnalysis implements Runnable
    {
        private IBGAnalyzer analyzer = null;
        private InteractionLogger logger = null;

        private Object taskMonitor = new Object();
        private State state = State.IDLE;

        private enum State
        { IDLE, PENDING, DONE };

        public ThreadIBGAnalysis()
        {
        }

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

                while (!done) {
                    switch (analyzer.analysisStep(logger, true)) {
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

                synchronized (taskMonitor) {
                    state = State.DONE;
                    taskMonitor.notify();
                }
            }
        }

        /*
         * tell the analysis thread to start analyzing, and return immediately
         */
        public void startAnalysis(IBGAnalyzer analyzer0, InteractionLogger logger0)
        {
            synchronized (taskMonitor) {
                if (state == State.PENDING) {
                    throw new RuntimeException("unexpected state in IBG startAnalysis");
                }

                analyzer = analyzer0;
                logger = logger0;
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
                logger = null;
                state = State.IDLE;
            }
        }
    }


    public static class ThreadIBGConstruction implements Runnable
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
                        } catch (InterruptedException e) {
                            throw new RuntimeException(e);
                        }
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
