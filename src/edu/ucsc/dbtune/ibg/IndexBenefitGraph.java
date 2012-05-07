package edu.ucsc.dbtune.ibg;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.util.Identifiable;

/**
 * An Index Benefit Graph (IBG) was introduced by Frank et al. [1]. An IBG enables a space-efficient 
 * encoding of the properties of optimal query plans when the optimizer is well behaved.
 * <p>
 * For a specific query {@latex.inline $q$} is a DAG over subsets of {@latex.inline S}. Each node 
 * represents an index-set {@latex.inline $Y \\subseteq S$} and records {@latex.inline $used_q(Y)$} 
 * and {@latex.inline $cost_q(Y)$}. The nodes and edges of the IBG are defined inductively as 
 * follows: The IBG contains the node {@latex.inline $S$}; For each node {@latex.inline $Y$} and 
 * each used index {@latex.inline $a \\in used_q(Y)$}, the IBG contains the node {@latex.inline 
 * $Y=Y−\\{a\\}$} and the directed {@latex.inline $edge(Y,Y)$}.
 * <p>
 * An example of an IBG looks like the following:
 * <pre>
 * {@code .
 *
 *       *a*,b,c,*d*:20
 *       /           \
 *    *a*,*b*,c:45   *b*,c,d:50
 *    /       \          \
 *  a,c:80  *b*,c:50   *c*,*d*:65
 *                |    /    |
 *                c:80     d:80
 * }
 * </pre>
 * <p>
 * One interesting observation is that {@latex.inline $bcd$} and {@latex.inline $bc$} differ by 
 * index {@latex.inline $d$}, yet no edge exists between them because {@latex.inline $d \\in 
 * used_q(bcd)$}. Also, notice that {@latex.inline $bcd \\triangleright bc$} and hence the two nodes 
 * are somewhat redundant with respect to information on optimal plans (but they are needed to 
 * complete the graph)
 * <p>
 * Because the IBG nodes only have one child per used index, the size of an IBG for a particular 
 * index-set can vary drastically. Some interesting ways to measure the size of an IBG are the 
 * number of nodes, the maximum children per node (i.e. fan-out), and the maximum path length (i.e. 
 * height). In the worst case, {@latex.inline $used(Y) = Y$} for each node {@latex.inline $Y$} and 
 * this results in a node for each subset of {@latex.inline $S$}, a fanout of {@latex.inline $|S|$}, 
 * and a height of {@latex.inline $|S|$}. However, in practice the optimizer may not use every index 
 * in {@latex.inline $Y$} (especially if {@latex.inline $Y$} is large), in which case the IBG can be 
 * much smaller. Indeed, The sample IBG given above contains only 8 of the 16 possible subsets, a 
 * fan-out of 2, and a height of 3.
 * <p>
 * This version of an IBG is introduced in [2].
 *
 * @author Karl Schnaitter
 * @author Ivo Jimenez
 * @see <a href="http://portal.acm.org/citation.cfm?id=649865">
 *     [1] Adaptive and Automated Index Selection in RDBMS</a>
 * @see <a href="http://portal.acm.org/citation.cfm?id=1687766">
 *     [2] Index interactions in physical design tuning: modeling, analysis, and applications</a>
 */
public class IndexBenefitGraph
{
    /** used to find nodes given. */
    private static IBGCoveringNodeFinder finder = new IBGCoveringNodeFinder();
    
    /**
     * The primary information stored by the graph.
     * 
     * Every node in the graph is a descendant of rootNode. We also keep the
     * cost of the query under the empty configuration, stored in emptyCost.
     */
    private final Node rootNode;

    /** cost of the empty configuration. */
    private double emptyCost;

    /**
     * Creates an IBG with the given root node, cost and usedSet.
     *
     * @param rootNode
     *     root node of the IBG
     * @param emptyCost
     *     cost associated to the empty configuration
     */
    public IndexBenefitGraph(Node rootNode, double emptyCost)
    {
        this.rootNode  = rootNode;
        this.emptyCost = emptyCost;
    }

    /**
     * Returns the cost associated to the empty configuration.
     *
     * @return
     *     the empty cost
     */
    public final double emptyCost()
    {
        return emptyCost;
    }
    
    /**
     * Returns the root node of the ibg.
     *
     * @return
     *     the empty cost
     */
    public final Node rootNode()
    {
        return rootNode;
    }

    /**
     * Finds the node corresponding to the given bitset.
     *
     * @param bitSet
     *     the configuration for which a node is being looked for
     * @return
     *     the corresponding node if found; {@code null} otherwise
     */
    public final Node find(Set<Index> bitSet)
    {
        return finder.find(rootNode(), bitSet);
    }

    /**
     * A node of the IBG.
     *
     * @author Karl Schnaitter
     */
    public static class Node implements Identifiable, Comparable<Node>
    {
        /** Configuration that this node is about. */
        private final Set<Index> config;
        private final Set<Index> used;

        /** id for the node that is unique within the enclosing IBG. */
        private final int id;

        /**
         * cost with the given configuration don't access until isExpanded() returns 
         * true. Internally, this is used to determine if the node is expanded... it 
         * is set to -1.0 until expanded
         */
        private volatile double cost;

        /** edges to children nodes. */
        private List<Edge> edges;

        /**
         * @param config0
         *     configuration
         * @param id0
         *     id of the node
         */
        public Node(Set<Index> config0, int id0)
        {
            config = new TreeSet<Index>(config0);
            id = id0;
            cost = -1.0;
            edges = new ArrayList<Edge>();
            used = new TreeSet<Index>();
        }

        /**
         * Returns the configuration corresponding to the node.
         *
         * @return
         *    the configuration corresponding to the node
         */
        public final Set<Index> getConfiguration()
        {
            return new TreeSet<Index>(config);
        }

        /**
         * Returns the ID for the node (unique within the enclosing IBG).
         *
         * @return
         *    the unique ID (with respect to the enclosing IBG) of the node.
         */
        @Override
        public final int getId()
        {
            return id;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public int compareTo(Node i)
        {
            return (new Integer(getId())).compareTo(i.getId());
        }

        /**
         * Check if it has children/cost yet.
         *
         * @return
         *     whether or not the node is expanded.
         */
        public final boolean isExpanded()
        {
            return cost >= 0;
        }

        /**
         * Adds a children.
         *
         * @param child
         *     new children
         * @param usedIndex
         *     index corresponding to the edge that goes from the parent to the given child
         */
        public final void addChild(Node child, Index usedIndex)
        {
            assert !isExpanded();

            edges.add(new Edge(child, usedIndex));

            used.add(usedIndex);
        }

        /**
         * Return the used indexes from this node.
         * @return The {@link Set} denoting the used indexes
         */
        public final Set<Index> getUsedIndexes()
        {
            assert isExpanded();
            return new TreeSet<Index>(used);
        }

        /**
         * Get the cost.
         *
         * @return
         *     the cost associated to the node
         */
        public final double cost()
        {
            assert isExpanded();
            return cost;
        }

        /**
         * Get the children nodes.
         *
         * @return
         *      the children nodes
         */
        public final List<Node> getChildren()
        {
            List<Node> nodes = new ArrayList<Node>();

            for (Edge e : edges)
                nodes.add(e.node);

            return nodes;
        }

        /**
         * Assigns the cost.
         *
         * @param cost0
         *     cost of the node.
         */
        public void setCost(double cost0)
        {
            cost = cost0;
        }

        /**
         */
        public static class Edge
        {
            //private final Node from;
            //private final Node to;
            private final Node node;
            private final Index usedIndex;

            /**
             * @param node
             *     node that the edge points to
             * @param usedIndex
             *     index assigned to the edge
             */
            public Edge(Node node, Index usedIndex)
            {
                this.node = node;
                this.usedIndex = usedIndex;
            }

            /**
             * Gets the usedIndex for this instance.
             *
             * @return The usedIndex.
             */
            public Index getUsedIndex()
            {
                return this.usedIndex;
            }

            /**
             * {@inheritDoc}
             */
            @Override
            public String toString()
            {
                return usedIndex + "=>" + node.id;
            }
        }

        /**
         * Gets the edges for this instance.
         *
         * @return The edges.
         */
        public List<Edge> getEdges()
        {
            return this.edges;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public boolean equals(Object other)
        {
            if (this == other)
                return true;

            if (!(other instanceof Node))
                return false;

            Node o = (Node) other;

            if (id == o.id)
                return true;

            return false;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public int hashCode()
        {
            return new Integer(id).hashCode();
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public String toString()
        {
            StringBuilder str = new StringBuilder();

            str.append(
                "ID: " + id + "; config: " + config + "; cost: " + cost + "; edges: " + edges);

            return str.toString();
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString()
    {
        StringBuilder str = new StringBuilder();

        TreeSet<Node> allNodes = new TreeSet<Node>();

        getAllNodes(rootNode, allNodes);

        for (Node n : allNodes)
            str.append(n + "\n");

        return str.toString();
    }

    /**
     * prints the node in a string.
     *
     * @param node
     *      node to be printed
     * @param visited
     *      list of visited nodes
     */
    private void getAllNodes(IndexBenefitGraph.Node node, Set<Node> visited)
    {
        if (!visited.contains(node))
            visited.add(node);

        for (IndexBenefitGraph.Node ch : node.getChildren())
            getAllNodes(ch, visited);
    }

    /**
     * Assigns the value of the empty cost. Only used by {@link MonotonicEnforcer}.
     *
     * @param cost
     *     the cost of the empty node.
     */
    void setEmptyCost(double cost)
    {
        emptyCost = cost;
    }
}
