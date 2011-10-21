/* ************************************************************************** *
 *   Copyright 2010 University of California Santa Cruz                       *
 *                                                                            *
 *   Licensed under the Apache License, Version 2.0 (the "License");          *
 *   you may not use this file except in compliance with the License.         *
 *   You may obtain a copy of the License at                                  *
 *                                                                            *
 *       http://www.apache.org/licenses/LICENSE-2.0                           *
 *                                                                            *
 *   Unless required by applicable law or agreed to in writing, software      *
 *   distributed under the License is distributed on an "AS IS" BASIS,        *
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied  *
 *   See the License for the specific language governing permissions and      *
 *   limitations under the License.                                           *
 * ************************************************************************** */
package edu.ucsc.dbtune.ibg;

import edu.ucsc.dbtune.advisor.interactions.InteractionLogger;
import edu.ucsc.dbtune.ibg.IndexBenefitGraph.IBGChild;
import edu.ucsc.dbtune.ibg.IndexBenefitGraph.IBGNode;
import edu.ucsc.dbtune.ibg.IndexBenefitGraphConstructor;
import edu.ucsc.dbtune.metadata.Configuration;
import edu.ucsc.dbtune.metadata.ConfigurationBitSet;
import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.optimizer.ExplainedSQLStatement;
import edu.ucsc.dbtune.optimizer.Optimizer;
import edu.ucsc.dbtune.util.IndexBitSet;
import edu.ucsc.dbtune.util.DefaultQueue;
import edu.ucsc.dbtune.workload.SQLStatement;

import java.sql.SQLException;
import java.util.HashSet;

/**
 * @author Karl Schnaitter
 * @author Huascar Sanchez
 * @author Ivo Jimenez
 */
public class IndexBenefitGraphConstructor {
	/* 
	 * Parameters of the construction.
	 */
    protected final Optimizer optimizer;
	protected SQLStatement sql;
	protected CandidatePool.Snapshot candidateSet;
    protected ConfigurationBitSet configuration;

    /* Counter for assigning unique node IDs */
	private int nodeCount;

    /* number of optimization calls done on the underlaying delegate optimizer */
    private int optCount;

	/* Temporary bit sets allocated once to allow reuse... only used in BuildNode */ 
	private IndexBitSet usedBitSet;
	private IndexBitSet childBitSet;
	
	/*
	 * The primary information stored by the graph
	 * 
	 * Every node in the graph is a descendant of rootNode. We also keep the
	 * cost of the workload under the empty configuration, stored in emptyCost.
	 */
	private IBGNode rootNode;

    /* cost without indexes */
	private double emptyCost;
	
	/* The queue of pending nodes to expand */
	DefaultQueue<IBGNode> queue;
	
	/* A monitor for waiting on a node expansion */
	private final Object nodeExpansionMonitor = new Object();

	/* An object that allows for covering node searches */
	private final IBGCoveringNodeFinder coveringNodeFinder = new IBGCoveringNodeFinder();
	
	/* true if the index is used somewhere in the graph */
	private IndexBitSet isUsed;
	
    /**
     * Creates an IBG constructor that uses the given optimizer to execute optimization calls.
     *
     * @param optimizer
     *     optimizer delegate that is being used.
	 */
    public IndexBenefitGraphConstructor(
            Optimizer optimizer,
            SQLStatement sql,
            ConfigurationBitSet conf)
        throws SQLException
    {
        this.optimizer     = optimizer;
        this.sql           = sql;
        this.configuration = conf;

        // set up the root node, and initialize the queue
        IndexBitSet rootConfig = configuration.getBitSet();
        rootNode = new IBGNode(rootConfig, nodeCount++);

        emptyCost = optimizer.explain(this.sql).getCost();

        // initialize the queue
        queue.add(rootNode);
    }
	
	public final double emptyCost() {
		return emptyCost;
	}
	
	public final IBGNode rootNode() {
		return rootNode;
	}
	
	public final int nodeCount() {
		return nodeCount;
	}

	public final int getOptimizationCount() {
		return optCount;
	}

	public final boolean isUsed(int i) {
		return isUsed.get(i);
	}
	
	public final Configuration candidateSet() {
		return configuration;
	}

	/*
	 * Wait for a specific node to be expanded
	 */
    public final void waitUntilExpanded(IBGNode node)
    {
        synchronized (nodeExpansionMonitor) {
            while (!node.isExpanded())
                try {
                    nodeExpansionMonitor.wait();
                } catch (InterruptedException e) {
                }
        }
    }

	/*
	 * Expands one node of the IBG. Returns true if a node was expanded, or false if there are no unexpanded nodes.
	 * This function is not safe to be called from more than one thread.
	 */
	public boolean buildNode() throws SQLException {
		IBGNode newNode, coveringNode;
        ExplainedSQLStatement sqlP; // may be useful to store the prepared statement to use it later
		double totalCost;

		if (queue.isEmpty())
			return false;
		
		newNode = queue.remove();
		
		// get cost and used set (stored into usedBitSet)
		usedBitSet.clear();
		coveringNode = coveringNodeFinder.find(rootNode, newNode.config);
		if (coveringNode != null)
        {
			totalCost = coveringNode.cost();
			coveringNode.addUsedIndexes(usedBitSet);
		}
		else
        {
            // Translation from Karl's framework to DBTune:
            //   - transform newNode.config to a Configuration object
            //   - call the optimizer on it
            //   - turn on the elements in usedBitSet object according to getUsedConfiguration()
            Configuration newNodeConf = new Configuration("from_bitset");

            for (Index idx : configuration)
                if (newNode.config.get(configuration.getOrdinalPosition(idx)))
                    newNodeConf.add(idx);

            optCount++;
            sqlP = optimizer.explain(sql, newNodeConf);

            // XXX: we may want to keep track of the PreparedSQLStatement for each of the 
            // corresponding optimization calls

            totalCost = sqlP.getCost();

            for (Index idx : sqlP.getUsedConfiguration())
                usedBitSet.set(configuration.getOrdinalPosition(idx));
		}
		
		// create the child list
		// if any IBGNode did not exist yet, add it to the queue
		// We make sure to keep the child list in the same order as the nodeQueue, so that
		// analysis and construction can move in lock step. This is done by keeping both
		// in order of construction.
		IBGChild firstChild = null;
		IBGChild lastChild = null;
		childBitSet.set(newNode.config);
		for (int u = usedBitSet.nextSetBit(0); u >= 0; u = usedBitSet.nextSetBit(u+1)) {
			childBitSet.clear(u);
			IBGNode childNode = find(queue, childBitSet);
			if (childNode == null) {
				isUsed.set(u);
				childNode = new IBGNode(childBitSet.clone(), nodeCount++);
				queue.add(childNode);
			}
			childBitSet.set(u);
			
			IBGChild child = new IBGChild(childNode, u);
			if (firstChild == null) {
				firstChild = lastChild = child;
			}
			else {
				lastChild.next = child;
				lastChild = child;
			}
		}
		
		// Expand the node and notify waiting threads
		synchronized (nodeExpansionMonitor) {
			newNode.expand(totalCost, firstChild);
			nodeExpansionMonitor.notifyAll();
		}
		
		return !queue.isEmpty();
	}
	
	/*
	 * Auxiliary method for buildNodes
	 */
	private static IBGNode find(DefaultQueue<IBGNode> queue, IndexBitSet config) {
		for (int i = 0; i < queue.count(); i++) {
			IBGNode node = queue.fetch(i);
			if (node.config.equals(config))
				return node;
		}
		return null;
	}

	public void setEmptyCost(double cost) {
		emptyCost = cost;
	}

    public static class CandidatePool
    {
        /* serializable fields */
        Node firstNode;
        Configuration configuration;
        HashSet<Index> indexSet;
        int size;

        public CandidatePool(Configuration conf) {
            firstNode = null;
            indexSet = new HashSet<Index>();
            size = -1;
            configuration = conf;
        }

        public final void addIndex(Index index) throws SQLException {
            if (!indexSet.contains(index)) {
                ++size;

                firstNode = new Node(index, firstNode);
                indexSet.add(index);
            }
        }

        public void addIndexes(Iterable<Index> newIndexes) throws SQLException {
            for (Index index : newIndexes)
                addIndex(index);
        }

        public final boolean isEmpty() {
            return firstNode == null;
        }

        public final boolean contains(Index index) {
            return indexSet.contains(index);
        }

        public Snapshot getSnapshot() {
            return new Snapshot(configuration, firstNode);
        }

        public java.util.Iterator<Index> iterator() {
            return new Iterator<Index>(firstNode);
        }

        private class Node {
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
        public static class Snapshot implements Iterable<Index> {
            /* serializable fields */
            int maxId;
            Node first;
            IndexBitSet bs;

            protected Snapshot() { }

            private Snapshot(Configuration conf, Node first0) {
                maxId = (first0 == null) ? -1 : conf.getOrdinalPosition(first0.index);
                first = first0;
                bs = new IndexBitSet();
                bs.set(0, maxId+1);
            }

            public java.util.Iterator<Index> iterator() {
                return new Iterator<Index>(first);
            }

            public int size() {
                return maxId;
            }

            public IndexBitSet bitSet() {
                return bs; // no need to clone -- this set is immutable
            }
        }

        /*
         * Iterator for a snapshot of the candidate set
         */
        private static class Iterator<I> implements java.util.Iterator<Index> {
            Node next;

            Iterator(Node start) {
                next = start;
            }

            public boolean hasNext() {
                return next != null;
            }

            public Index next() {
                if (next == null)
                    throw new java.util.NoSuchElementException();
                Index current = next.index;
                next = next.next;
                return current;
            }

            public void remove() {
                throw new UnsupportedOperationException();
            }
        }
    }

    public static class ThreadIBGAnalysis implements Runnable {
        private IBGAnalyzer analyzer = null;
        private InteractionLogger logger = null;

        private Object taskMonitor = new Object();
        private State state = State.IDLE;

        private enum State { IDLE, PENDING, DONE };

        public ThreadIBGAnalysis() {
        }

        public void run() {
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

                //int analyzedCount = 0;

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
        public void startAnalysis(IBGAnalyzer analyzer0, InteractionLogger logger0) {
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

        public void waitUntilDone() {
            synchronized (taskMonitor) {
                while (state == State.PENDING) {
                    try {
                        taskMonitor.wait();
                    } catch (InterruptedException e) { }
                }

                analyzer = null;
                logger = null;
                state = State.IDLE;
            }
        }
    }


    public static class ThreadIBGConstruction implements Runnable {
        private IndexBenefitGraphConstructor ibgCons = null;

        private Object taskMonitor = new Object();
        private State state = State.IDLE;

        private enum State { IDLE, PENDING, DONE };

        public ThreadIBGConstruction() {
        }

        public void run() {
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
                    boolean success; // bad name for this variable, should be called "moreNodesToBuild" or something to that effect
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
        public void startConstruction(IndexBenefitGraphConstructor ibgCons0) {
            synchronized (taskMonitor) {
                if (state == State.PENDING) {
                    throw new RuntimeException("unexpected state in IBG startConstruction");
                }

                ibgCons = ibgCons0;
                state = State.PENDING;
                taskMonitor.notify();
            }
        }

        public void waitUntilDone() {
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

    /**
     * Creates an IBG which is in a state ready for building. Specifically, the rootNode is 
     * physically constructed, but it is not expanded, so its cost and used set have not been 
     * determined.
     * 
     * In the initial state, the cost of the workload under the empty configuration is set, and may 
     * be accessed through emptyCost().
     * 
     * Nodes are built by calling buildNode() until it returns false.
     *
     * @param sql
     *     statement being explained
     * @param conf
     *     configuration to take into account
     */
    private void initializeConstruction(
            SQLStatement sql0,
            ConfigurationBitSet conf)
        throws SQLException
    {
        IndexBitSet rootConfig;
        CandidatePool pool = new CandidatePool(conf);

        sql           = sql0;
        configuration = conf;
        optCount      = 0;
        nodeCount     = 0;
        usedBitSet    = new IndexBitSet();
        childBitSet   = new IndexBitSet();
        isUsed        = new IndexBitSet();
        candidateSet  = pool.getSnapshot();
        rootConfig    = candidateSet.bitSet(); // set up the root node, and initialize the queue
        queue         = new DefaultQueue<IBGNode>();
        rootNode      = new IBGNode(rootConfig, nodeCount++);
        emptyCost     = optimizer.explain(sql).getCost();

        optCount++;

        // initialize the queue
        queue.add(rootNode);
    }

    /**
     * Construct an IBG.
     */
    public IndexBenefitGraph constructIBG(Configuration configuration)
        throws SQLException
    {
        ThreadIBGAnalysis ibgAnalysis = new ThreadIBGAnalysis();
        // XXX: hide the interaction bank in here, something like:
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
        //long nStart = System.nanoTime();
        ibgAnalysis.waitUntilDone();
        //long nStop = System.nanoTime();

        //ProfiledQuery<I> qinfo =
        //  new ProfiledQuery(
        //    sql,
        //    explainInfo,
        //    indexes,
        //    ibgCons.getIBG(),
        //    logger.getInteractionBank(),
        //    conn.whatifCount(),
        //    ((nStop - nStart) / 1000000.0));

		IndexBenefitGraph ibg =
            new IndexBenefitGraph(rootNode, emptyCost, isUsed); //, ibgAnalysis);

        return ibg;
    }

    /**
     * Construct an IBG from the given parameters.
     *
     * @param sql
     *     statement being explained
     * @param conf
     *     configuration to take into account
     */
    public static IndexBenefitGraph construct(
            Optimizer delegate,
            SQLStatement sql,
            ConfigurationBitSet conf)
        throws SQLException
    {
        IndexBenefitGraphConstructor ibgConstructor =
            new IndexBenefitGraphConstructor(delegate,sql,conf);
        // XXX: there's a correspondence between the ordinal position of an index (with respect to a 
        // Configuration object) and bitSet-based operations done throughout the edu.ucsc.dbtune.ibg 
        // package, including construction and what-if optimization done on top of an IBG. Inside 
        // the IBG package, when bitSet.set(i) is stated, this is making a reference to the i-th 
        // index contained in a Configuration object (where 0 - first index; 1 - second index; 
        // etc.). For a given index idx, the following should be true:
        //
        //    bitSet.set(i) == conf.getOrdinalPosition(idx)
        //
        // the above should hold EVERYWHERE in the edu.ucsc.dbtune.ibg package, otherwise we're in 
        // trouble.
        ibgConstructor.initializeConstruction(sql,conf);

        return ibgConstructor.constructIBG(conf);
    }
}
