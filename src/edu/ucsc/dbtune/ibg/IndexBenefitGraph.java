package edu.ucsc.dbtune.ibg;

import edu.ucsc.dbtune.util.IndexBitSet;

/**
 * An Index Benefit Graph (IBG) was introduced by Frank et al.. An IBG enables a space-efficient 
 * encoding of the properties of optimal query plans when the optimizer is well behaved.
 * <p>
 * For a specific query $q$ is a DAG over subsets of S. Each node represents an index-set $Y ⊆ S$ 
 * and records $used_q(Y)$ and $cost_q(Y)$. The nodes and edges of the IBG are defined inductively 
 * as follows: The IBG contains the node $S$; For each node $Y$ and each used index $a ∈ used_q(Y)$, 
 * the IBG contains the node $Y=Y−{a}$ and the directed $edge(Y,Y)$.
 * <p>
 * An example of an IBG looks like the following:
 * <code>
 *       *a*,b,c,*d*:20
 *       /           \
 *    *a*,*b*,c:45   *b*,c,d:50
 *    /       \          \
 *  a,c:80  *b*,c:50   *c*,*d*:65
 *                |    /    |
 *                c:80     d:80
 * </code>
 * <p>
 * One interesting observation is that $bcd$ and $bc$ differ by index $d$, yet no edge exists 
 * between them because $d \in used_q(bcd)$. Also, notice that $bcd \triangleright bc$ and hence the 
 * two nodes are somewhat redundant with respect to information on optimal plans (but they are 
 * needed to complete the graph)
 * <p>
 * Because the IBG nodes only have one child per used index, the size of an IBG for a particular 
 * index-set can vary drastically. Some interesting ways to measure the size of an IBG are the 
 * number of nodes, the maximum children per node (i.e. fan-out), and the maximum path length (i.e. 
 * height). In the worst case, $used(Y) = Y$ for each node $Y$ and this results in a node for each 
 * subset of $S$, a fanout of $|S|$, and a height of $|S|$. However, in practice the optimizer may 
 * not use every index in $Y$ (especially if $Y$ is large), in which case the IBG can be much 
 * smaller. Indeed, The sample IBG given above contains only 8 of the 16 possible subsets, a fan-out 
 * of 2, and a height of 3.
 *
 * @author Karl Schnaitter
 * @author Ivo Jimenez
 * @see <a href="http://portal.acm.org/citation.cfm?id=1687766">
 *     Index interactions in physical design tuning: modeling, analysis, and applications</a>
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
    private final IBGNode rootNode;

    /** cost of the empty configuration. */
    private double emptyCost;

    /** a bit is set if the corresponding index is used somewhere in the graph. */
    private final IndexBitSet isUsed;

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
    public IndexBenefitGraph(IBGNode rootNode, double emptyCost, IndexBitSet isUsed)
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
    public final IBGNode rootNode()
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
    public final IBGNode find(IndexBitSet bitSet)
    {
        return finder.findFast(rootNode(), bitSet, null);
    }

    /**
     * Whether or not the position of an index (in the underlaying bit array) corresponds to an 
     * index that is used by some node of the graph.
     *
     * @param position
     *     the position of an index in the underlaying bitset.
     * @return
     *     {@code true} if the corresponding index is used; {@code false} otherwise.
     */
    public final boolean isUsed(int position)
    {
        return isUsed.contains(position);
    }

    /**
     * A node of the IBG.
     *
     * @author Karl Schnaitter
     */
    public static class IBGNode
    {
        /** Configuration that this node is about. */
        private final IndexBitSet config;

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
        private volatile IBGChild firstChild;

        /**
         * Used indexes.
         * Don't access until isExpanded() returns true
         */
        private volatile IndexBitSet usedIndexes;

        /**
         * @param config0
         *     configuration
         * @param id0
         *     id of the node
         */
        public IBGNode(IndexBitSet config0, int id0)
        {
            config      = config0;
            id          = id0;
            cost        = -1.0;
            firstChild  = null;
            usedIndexes = new IndexBitSet();
        }

        /**
         * Returns the configuration corresponding to the node.
         *
         * @return
         *    the configuration corresponding to the node
         */
        public final IndexBitSet getConfiguration()
        {
            return config;
        }

        /**
         * Returns the ID for the node (unique within the enclosing IBG).
         *
         * @return
         *    the unique ID (with respect to the enclosing IBG) of the node.
         */
        public final int getID()
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
         * @param cost0
         *     the cost assigned to the node.
         * @param firstChild0
         *     the first child in the list of childs
         */
        public final void expand(double cost0, IBGChild firstChild0)
        {
            assert !isExpanded();
            assert cost0 >= 0;

            // volatile assignments must be ordered with "state" assigned last
            cost = cost0;
            firstChild = firstChild0;
            addUsedIndexes(usedIndexes);
        }

        /**
         * Return the used indexes from this node.
         * @return The {@link IndexBitSet} denoting the used indexes
         */
        public final IndexBitSet getUsedIndexes()
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
        protected final IBGChild firstChild()
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
        public final void addUsedIndexes(IndexBitSet other)
        {
            assert isExpanded();

            for (IBGChild ch = firstChild; ch != null; ch = ch.next)
                other.add(ch.usedIndex);
        }

        /**
         * Remove each of the used indexes in this node from the given IndexBitSet.
         *
         * @param other
         *      other configuration
         */
        public void clearUsedIndexes(IndexBitSet other)
        {
            assert isExpanded();

            for (IBGChild ch = firstChild; ch != null; ch = ch.next)
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
        public boolean usedSetIsSubsetOf(IndexBitSet other)
        {
            assert isExpanded();

            for (IBGChild ch = firstChild; ch != null; ch = ch.next)
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
        public boolean usedSetContains(int id)
        {
            assert isExpanded();

            for (IBGChild ch = firstChild; ch != null; ch = ch.next)
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
        public static class IBGChild
        {
            /** the internal id of the index assigned to the edge of this child. */
            private final int usedIndex;

            /** the actual child node. */
            private final IBGNode node;

            /** the next child in the linked list. */
            private IBGChild next;

            /**
             * @param node0
             *     node corresponding to the child
             * @param usedIndex0
             *     index used on the edge
             */
            public IBGChild(IBGNode node0, int usedIndex0)
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

                if (!(other instanceof IBGChild))
                    return false;

                IBGChild ibgchild = (IBGChild) other;

                if (ibgchild.usedIndex == usedIndex && ibgchild.node.id == node.id)
                    return true;

                return false;
            }

            /**
             * Gets the usedIndex for this instance.
             *
             * @return The usedIndex.
             */
            public int getUsedIndex()
            {
                return this.usedIndex;
            }

            /**
             * Gets the node for this instance.
             *
             * @return The node.
             */
            public IBGNode getNode()
            {
                return this.node;
            }

            /**
             * Sets the next for this instance.
             *
             * @param next The next.
             */
            public void setNext(IBGChild next)
            {
                this.next = next;
            }

            /**
             * Gets the next for this instance.
             *
             * @return The next.
             */
            public IBGChild getNext()
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

                for (IBGChild ch = next; ch != null; ch = ch.next)
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

            if (!(other instanceof IBGNode))
                return false;

            IBGNode o = (IBGNode) other;

            if (!config.equals(o.config) || cost != o.cost)
                return false;

            if (id != o.id)
                return false;

            IBGChild ch;
            IBGChild cho;

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
