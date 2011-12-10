/* **************************************************************************** *
 *   Copyright 2010 University of California Santa Cruz                         *
 *                                                                              *
 *   Licensed under the Apache License, Version 2.0 (the "License");            *
 *   you may not use this file except in compliance with the License.           *
 *   You may obtain a copy of the License at                                    *
 *                                                                              *
 *       http://www.apache.org/licenses/LICENSE-2.0                             *
 *                                                                              *
 *   Unless required by applicable law or agreed to in writing, software        *
 *   distributed under the License is distributed on an "AS IS" BASIS,          *
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.   *
 *   See the License for the specific language governing permissions and        *
 *   limitations under the License.                                             *
 * **************************************************************************** */
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
 *       /          \
 *    *a*,*b*,c:45  *b*,c,d:50
 *    /       \         \
 *  a,c:80  *b*,c:50  *c*,*d*:65
 *               |   /     |
 *              c:80     d:80
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
    /**
     * The primary information stored by the graph.
     * 
     * Every node in the graph is a descendant of rootNode. We also keep the
     * cost of the query under the empty configuration, stored in emptyCost.
     */
    private final IBGNode rootNode;

    /**
     */
    private double emptyCost;

    /** a bit is set if the corresponding index is used somewhere in the graph */
    private final IndexBitSet isUsed;

    /** used to find nodes given */
    private static IBGCoveringNodeFinder FINDER = new IBGCoveringNodeFinder();
    
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
        return FINDER.findFast(rootNode(),bitSet,null);
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
    public final boolean isUsed(int i)
    {
        return isUsed.get(i);
    }

    /**
     * A node of the IBG
     *
     * @author Karl Schnaitter
     */
    public static class IBGNode
    {
        /** Configuration that this node is about */
        public final IndexBitSet config;

        /** id for the node that is unique within the enclosing IBG */
        public final int id;

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

        public IBGNode(IndexBitSet config0, int id0)
        {
            config     = config0;
            id         = id0;
            cost       = -1.0;
            firstChild = null;
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
         * Check if it has children/cost yet
         */
        protected final boolean isExpanded()
        {
            return cost >= 0;
        }

        /**
         * Set the cost and list of children (one for each used index).
         */
        public final void expand(double cost0, IBGChild firstChild0)
        {
            assert(!isExpanded());
            assert(cost0 >= 0);

            // volatile assignments must be ordered with "state" assigned last
            cost = cost0;
            firstChild = firstChild0;
            addUsedIndexes(usedIndexes);
        }

        /**
         * Get the cost
         */
        public final double cost()
        {
            assert(isExpanded());
            return cost;
        }

        /**
         * Get the head of the child list
         */
        protected final IBGChild firstChild()
        {
            assert(isExpanded());
            return firstChild; 
        }

        /**
         * Add each of the used indexes in this node to the given IndexBitSet
         */
        public final void addUsedIndexes(IndexBitSet bs)
        {
            assert(isExpanded());
            for (IBGChild ch = firstChild; ch != null; ch = ch.next)
                bs.set(ch.usedIndex);
        }

        /**
         * Remove each of the used indexes in this node from the given IndexBitSet
         */
        public void clearUsedIndexes(IndexBitSet bs)
        {
            assert(isExpanded());
            for (IBGChild ch = firstChild; ch != null; ch = ch.next)
                bs.clear(ch.usedIndex);
        }

        /**
         * return true if each of the used indexes are in the given IndexBitSet
         */
        public boolean usedSetIsSubsetOf(IndexBitSet other)
        {
            assert(isExpanded());
            for (IBGChild ch = firstChild; ch != null; ch = ch.next)
                if (!other.get(ch.usedIndex))
                    return false;
            return true;
        }

        /**
         * return true if the i is in the used set
         */
        public boolean usedSetContains(int id)
        {
            assert(isExpanded());
            for (IBGChild ch = firstChild; ch != null; ch = ch.next)
                if (id == ch.usedIndex)
                    return true;
            return false;
        }

        public void setCost(double cost0)
        {
            cost = cost0;
        }

        /**
         * Return the used indexes from this node.
         * @return The {@link IndexBitSet} denoting the used indexes
         */
        public final IndexBitSet getUsedIndexes() {
            assert(isExpanded());
            return usedIndexes;
        }

        public static class IBGChild
        {
            public final int usedIndex; // the internalID of the used index on this edge
            public final IBGNode node; // the actual child node
            public IBGChild next = null;

            // next pointer is initially null
            public IBGChild(IBGNode node0, int usedIndex0)
            {
                node = node0;
                usedIndex = usedIndex0;
            }
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
