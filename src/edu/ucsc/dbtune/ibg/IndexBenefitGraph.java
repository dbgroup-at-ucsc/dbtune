package edu.ucsc.dbtune.ibg;

import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.util.Identifiable;
import edu.ucsc.dbtune.util.BitArraySet;

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

    /** a bit is set if the corresponding index is used somewhere in the graph. */
    private final BitArraySet<Index> isUsed;

    /**
     * Creates an IBG with the given root node, cost and usedSet.
     *
     * @param rootNode
     *     root node of the IBG
     * @param emptyCost
     *     cost associated to the empty configuration
     * @param isUsed
     *     bit array associated with the used configuration ibg-wise.
     */
    public IndexBenefitGraph(Node rootNode, double emptyCost, BitArraySet<Index> isUsed)
    {
        this.rootNode  = rootNode;
        this.emptyCost = emptyCost;
        this.isUsed    = isUsed;
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
    public final Node find(BitArraySet<Index> bitSet)
    {
        return finder.findFast(rootNode(), bitSet, null);
    }

    /**
     * A node of the IBG.
     *
     * @author Karl Schnaitter
     */
    public static class Node implements Identifiable
    {
        /** Configuration that this node is about. */
        private final BitArraySet<Index> config;

        /** id for the node that is unique within the enclosing IBG. */
        private final int id;

        /**
         * cost with the given configuration don't access until isExpanded() returns 
         * true. Internally, this is used to determine if the node is expanded... it 
         * is set to -1.0 until expanded
         */
        private volatile double cost;

        /**
         * Linked list of children. Note: don't access until isExpanded() returns true
         */
        private volatile Child firstChild;

        /**
         * Used indexes.
         * Don't access until isExpanded() returns true
         */
        private volatile BitArraySet<Index> usedIndexes;

        /**
         * @param config0
         *     configuration
         * @param id0
         *     id of the node
         */
        public Node(BitArraySet<Index> config0, int id0)
        {
            config      = config0;
            id          = id0;
            cost        = -1.0;
            firstChild  = null;
            usedIndexes = new BitArraySet<Index>();
        }

        /**
         * Returns the configuration corresponding to the node.
         *
         * @return
         *    the configuration corresponding to the node
         */
        public final BitArraySet<Index> getConfiguration()
        {
            return config;
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
         * Check if it has children/cost yet.
         *
         * @return
         *     whether or not the node is expanded.
         */
        protected final boolean isExpanded()
        {
            return cost >= 0;
        }

        /**
         * Set the cost and list of children (one for each used index).
         *
         * @param cost
         *     the cost assigned to the node.
         * @param firstChild
         *     the first child in the list of childs
         */
        public final void expand(double cost, Child firstChild)
        {
            assert !isExpanded();
            assert cost >= 0;

            // volatile assignments must be ordered with "state" assigned last
            this.cost = cost;
            this.firstChild = firstChild;
            addUsedIndexes(usedIndexes);
        }

        /**
         * Return the used indexes from this node.
         * @return The {@link BitArraySet} denoting the used indexes
         */
        public final BitArraySet<Index> getUsedIndexes()
        {
            assert isExpanded();
            return usedIndexes;
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
         * Get the head of the child list.
         *
         * @return
         *      the first child in the linked list
         */
        protected final Child firstChild()
        {
            assert isExpanded();
            return firstChild; 
        }

        /**
         * Add each of the used indexes in this node to the given IndexBitSet.
         *
         * @param other
         *      other configuration
         */
        public final void addUsedIndexes(BitArraySet<Index> other)
        {
            assert isExpanded();

            for (Child ch = firstChild; ch != null; ch = ch.next)
                other.add(ch.usedIndex);
        }

        /**
         * Remove each of the used indexes in this node from the given IndexBitSet.
         *
         * @param other
         *      other configuration
         */
        public void clearUsedIndexes(BitArraySet<Index> other)
        {
            assert isExpanded();

            for (Child ch = firstChild; ch != null; ch = ch.next)
                other.remove(ch.usedIndex);
        }

        /**
         * return true if each of the used indexes are in the given IndexBitSet.
         *
         * @param other
         *      other configuration
         * @return
         *      {@code true} if each of the used indexes are in the given configuration; {@code 
         *      false} otherwise.
         */
        public boolean usedSetIsSubsetOf(BitArraySet<Index> other)
        {
            assert isExpanded();

            for (Child ch = firstChild; ch != null; ch = ch.next)
                if (!other.contains(ch.usedIndex))
                    return false;
            return true;
        }

        /**
         * return true if the given id of an index is in the used set.
         *
         * @param id
         *     the id of an index.
         * @return
         *      {@code true} if the given id is in the used set; {@code false} otherwise.
         */
        public boolean usedSetContains(Index id)
        {
            assert isExpanded();

            for (Child ch = firstChild; ch != null; ch = ch.next)
                if (id == ch.usedIndex)
                    return true;
            return false;
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
        public static class Child
        {
            /** the internal id of the index assigned to the edge of this child. */
            private final Index usedIndex;

            /** the actual child node. */
            private final Node node;

            /** the next child in the linked list. */
            private Child next;

            /**
             * @param node0
             *     node corresponding to the child
             * @param usedIndex0
             *     index used on the edge
             */
            public Child(Node node0, Index usedIndex0)
            {
                node = node0;
                usedIndex = usedIndex0;
            }

            /**
             * {@inheritDoc}
             */
            @Override
            public boolean equals(Object other)
            {
                if (other == this)
                    return true;

                if (!(other instanceof Child))
                    return false;

                Child ibgchild = (Child) other;

                if (ibgchild.usedIndex == usedIndex && ibgchild.node.id == node.id)
                    return true;

                return false;
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
             * Gets the node for this instance.
             *
             * @return The node.
             */
            public Node getNode()
            {
                return this.node;
            }

            /**
             * Sets the next for this instance.
             *
             * @param next The next.
             */
            public void setNext(Child next)
            {
                this.next = next;
            }

            /**
             * Gets the next for this instance.
             *
             * @return The next.
             */
            public Child getNext()
            {
                return this.next;
            }

            /**
             * {@inheritDoc}
             */
            @Override
            public int hashCode()
            {
                return node.hashCode();
            }

            /**
             * {@inheritDoc}
             */
            @Override
            public String toString()
            {
                String str = "idx:" + usedIndex + "-to-node:" + node.id;

                for (Child ch = next; ch != null; ch = ch.next)
                    str += "|idx:" + ch.usedIndex + "-to-node:" + ch.node.id;

                return str;
            }
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

            if (!config.equals(o.config) || cost != o.cost)
                return false;

            if (id != o.id)
                return false;

            Child ch;
            Child cho;

            for (ch = firstChild, cho = o.firstChild; ch != null; ch = ch.next, cho = cho.next)
                if (cho == null || !cho.equals(ch))
                    return false;

            if (cho != null)
                return false;

            return true;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public int hashCode()
        {
            return config.hashCode();
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public String toString()
        {
            return "ID: " + id + "; config: " + config + "; cost: " + cost + "; edges: " + 
                firstChild;
        }
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
