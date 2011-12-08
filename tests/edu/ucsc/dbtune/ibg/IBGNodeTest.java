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

import edu.ucsc.dbtune.ibg.IndexBenefitGraph.IBGNode;
import edu.ucsc.dbtune.util.IndexBitSet;

import org.junit.Test;
import org.junit.BeforeClass;

import static org.junit.Assert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.nullValue;

/**
 * Tests of IBG nodes
 *
 * @author Ivo Jimenez
 */
public class IBGNodeTest
{
    private static IBGNode root;
    private static IBGNode.IBGChild child1;
    private static IBGNode.IBGChild child2;
    private static IBGNode.IBGChild child3;
    private static IBGNode          node1;
    private static IBGNode          node2;
    private static IBGNode          node3;
    private static IndexBitSet      ibs0;
    private static IndexBitSet      ibs1;
    private static IndexBitSet      ibs2;
    private static IndexBitSet      ibs3;
    private static IndexBitSet ibs;
    private static IndexBitSet empty;
    private static IndexBitSet rootibs;
    private static IndexBitSet node2ibs;

    /**
     * Creates the following ibg:
     *
     *    (*a*,*b*,c):20
     *    /         \
     * (a,c):80    (*b*,c):50
     *               |
     *              (c):80
     */
    @BeforeClass
    public static void setUp() throws Exception
    {
        // create node (abc):20
        ibs0 = new IndexBitSet();

        ibs0.set(0);
        ibs0.set(1);
        ibs0.set(2);

        root = new IBGNode(ibs0, 0);

        root.setCost(20);

        // create node (ac):80
        ibs1 = new IndexBitSet();

        ibs1.set(0);
        ibs1.set(1);

        node1 = new IBGNode(ibs1, 1);

        node1.setCost(80);

        child1 = new IBGNode.IBGChild(node1,0);

        root.expand(20,child1);

        // create node (bc):50
        ibs2 = new IndexBitSet();

        ibs2.set(2);
        ibs2.set(3);

        node2 = new IBGNode(ibs2, 1);

        node2.setCost(50);

        child2 = new IBGNode.IBGChild(node2,1);

        child1.next = child2;

        // create node (c):80
        ibs3 = new IndexBitSet();

        ibs3.set(3);

        node3 = new IBGNode(ibs3, 1);

        node3.setCost(80);

        child3 = new IBGNode.IBGChild(node3,1);

        node2.expand(50,child3);

        ibs = new IndexBitSet();
        empty = new IndexBitSet();
        rootibs = new IndexBitSet();
        node2ibs = new IndexBitSet();

        rootibs.set(0);
        rootibs.set(1);

        node2ibs.set(1);

    }


    /**
     * Checks that the constructor operates appropriately
     */
    @Test
    public void testConstructor() throws Exception
    {
        IndexBitSet ibs = new IndexBitSet();
        IBGNode node    = new IBGNode(ibs, 10);

        assertThat(node.getConfiguration(), is(ibs));
        assertThat(node.getID(), is(10));
        assertThat(node.cost(), lessThan(0.0));
        assertThat(node.isExpanded(), is(false));
        assertThat(node.firstChild(), is(nullValue()));
    }

    @Test
    public void testExpansion() throws Exception
    {
        assertThat(root.isExpanded(),is(true));
        assertThat(node1.isExpanded(),is(true));
        assertThat(node2.isExpanded(),is(true));
        assertThat(node3.isExpanded(),is(true));
    }

    @Test
    public void testInternalBitSet() throws Exception
    {
        assertThat(root.getConfiguration(),is(ibs0));
        assertThat(node1.getConfiguration(),is(ibs1));
        assertThat(node2.getConfiguration(),is(ibs2));
        assertThat(node3.getConfiguration(),is(ibs3));
    }

    @Test
    public void testCostAssignment() throws Exception
    {
        assertThat(root.cost(),is(20.0));
        assertThat(node1.cost(),is(80.0));
        assertThat(node2.cost(),is(50.0));
        assertThat(node3.cost(),is(80.0));
    }

    @Test
    public void testStructure() throws Exception
    {
        assertThat(root.firstChild().node,is(node1));
        assertThat(root.firstChild().next.node,is(node2));
        assertThat(node1.firstChild(),is(nullValue()));
        assertThat(node2.firstChild().node,is(node3));
        assertThat(node3.firstChild(),is(nullValue()));
    }

    @Test
    public void testEdges() throws Exception
    {
        assertThat(root.firstChild().usedIndex, is(0));
        assertThat(root.firstChild().next.usedIndex, is(1));
        assertThat(node2.firstChild().usedIndex,is(1));
    }

    @Test
    public void testUsedAndClearIndexes() throws Exception
    {
        root.addUsedIndexes(ibs);
        assertThat(ibs,is(rootibs));
        root.clearUsedIndexes(ibs);
        assertThat(ibs,is(empty));

        node1.addUsedIndexes(ibs);
        assertThat(ibs,is(empty));
        node1.clearUsedIndexes(ibs);
        assertThat(ibs,is(empty));

        node2.addUsedIndexes(ibs);
        assertThat(ibs,is(node2ibs));
        node2.clearUsedIndexes(ibs);
        assertThat(ibs,is(empty));

        node3.addUsedIndexes(ibs);
        assertThat(ibs,is(empty));
        node3.clearUsedIndexes(ibs);
        assertThat(ibs,is(empty));
    }

    @Test
    public void testContainment() throws Exception
    {
        assertThat(node1.usedSetIsSubsetOf(rootibs), is(true));
        assertThat(node2.usedSetIsSubsetOf(rootibs), is(true));
        assertThat(node3.usedSetIsSubsetOf(rootibs), is(true));
        assertThat(node1.usedSetIsSubsetOf(node2ibs), is(true));
        assertThat(node3.usedSetIsSubsetOf(node2ibs), is(true));

        assertThat(root.usedSetContains(0), is(true));
        assertThat(root.usedSetContains(1), is(true));
        assertThat(root.usedSetContains(2), is(false));

        assertThat(node1.usedSetContains(0), is(false));
        assertThat(node1.usedSetContains(1), is(false));
        assertThat(node1.usedSetContains(2), is(false));

        assertThat(node2.usedSetContains(0), is(false));
        assertThat(node2.usedSetContains(1), is(true));
        assertThat(node2.usedSetContains(2), is(false));

        assertThat(node3.usedSetContains(0), is(false));
        assertThat(node3.usedSetContains(1), is(false));
        assertThat(node3.usedSetContains(2), is(false));
    }
}
