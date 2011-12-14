package edu.ucsc.dbtune.ibg;

import edu.ucsc.dbtune.ibg.IndexBenefitGraph.IBGNode;
import edu.ucsc.dbtune.util.IndexBitSet;

import org.junit.BeforeClass;
import org.junit.Test;

import static edu.ucsc.dbtune.DBTuneInstances.configureIndexBenefitGraph;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;

/**
 * Tests of IBG nodes.
 *
 * @author Ivo Jimenez
 */
public class IBGNodeTest
{
    // CHECKSTYLE:OFF
    private static IndexBenefitGraph ibg;
    private static IBGNode           root;
    private static IBGNode           node1;
    private static IBGNode           node2;
    private static IBGNode           node3;
    private static IBGNode           node4;
    private static IBGNode           node5;
    private static IBGNode           node6;
    private static IBGNode           node7;
    private static IBGNode.IBGChild  child1;
    private static IBGNode.IBGChild  child2;
    private static IBGNode.IBGChild  child3;
    private static IBGNode.IBGChild  child4;
    private static IBGNode.IBGChild  child5;
    private static IBGNode.IBGChild  child6;
    private static IBGNode.IBGChild  child7;
    private static IBGNode.IBGChild  child8;
    private static IBGNode.IBGChild  tmpchild1;
    private static IBGNode.IBGChild  tmpchild2;
    private static IBGNode.IBGChild  tmpchild3;
    private static IBGNode.IBGChild  tmpchild4;
    private static IBGNode.IBGChild  tmpchild5;
    private static IBGNode.IBGChild  tmpchild6;
    private static IBGNode.IBGChild  tmpchild7;
    private static IBGNode.IBGChild  tmpchild8;
    private static IndexBitSet       empty;
    private static IndexBitSet       rootibs;
    private static IndexBitSet       ibs1;
    private static IndexBitSet       ibs2;
    private static IndexBitSet       ibs3;
    private static IndexBitSet       ibs4;
    private static IndexBitSet       ibs5;
    private static IndexBitSet       ibs6;
    private static IndexBitSet       ibs7;
    private static IndexBitSet       usedr;
    private static IndexBitSet       used1;
    private static IndexBitSet       used2;
    private static IndexBitSet       used3;
    private static IndexBitSet       used4;
    private static IndexBitSet       used5;
    private static IndexBitSet       used6;
    private static IndexBitSet       used7;
    // CHECKSTYLE:ON

    /**
     * Creates the IBG under test.
     */
    @BeforeClass
    public static void setUp()
    {
        rootibs = new IndexBitSet();
        empty   = new IndexBitSet();
        ibs1    = new IndexBitSet();
        ibs2    = new IndexBitSet();
        ibs3    = new IndexBitSet();
        ibs4    = new IndexBitSet();
        ibs5    = new IndexBitSet();
        ibs6    = new IndexBitSet();
        ibs7    = new IndexBitSet();
        usedr   = new IndexBitSet();
        used1   = new IndexBitSet();
        used2   = new IndexBitSet();
        used3   = new IndexBitSet();
        used4   = new IndexBitSet();
        used5   = new IndexBitSet();
        used6   = new IndexBitSet();
        used7   = new IndexBitSet();

        ibg    = configureIndexBenefitGraph();

        child1 = ibg.rootNode().firstChild();
        child2 = child1.next;
        child3 = child1.node.firstChild();
        child4 = child3.next;
        child5 = child2.node.firstChild();
        child6 = child4.node.firstChild();
        child7 = child5.node.firstChild();
        child8 = child7.next;

        rootibs.set(0);
        rootibs.set(1);
        rootibs.set(2);
        rootibs.set(3);

        ibs1.set(0);
        ibs1.set(1);
        ibs1.set(2);

        ibs2.set(1);
        ibs2.set(2);
        ibs2.set(3);

        ibs3.set(0);
        ibs3.set(2);

        ibs4.set(1);
        ibs4.set(2);

        ibs5.set(2);
        ibs5.set(3);

        ibs6.set(2);

        ibs7.set(3);

        root  = new IBGNode(rootibs, 0);
        node1 = new IBGNode(ibs1, 1);
        node2 = new IBGNode(ibs2, 2);
        node3 = new IBGNode(ibs3, 3);
        node4 = new IBGNode(ibs4, 4);
        node5 = new IBGNode(ibs5, 5);
        node6 = new IBGNode(ibs6, 6);
        node7 = new IBGNode(ibs7, 7);

        tmpchild1 = new IBGNode.IBGChild(node1, 3);
        tmpchild2 = new IBGNode.IBGChild(node2, 0);
        tmpchild3 = new IBGNode.IBGChild(node3, 1);
        tmpchild4 = new IBGNode.IBGChild(node4, 0);
        tmpchild5 = new IBGNode.IBGChild(node5, 1);
        tmpchild6 = new IBGNode.IBGChild(node6, 1);
        tmpchild7 = new IBGNode.IBGChild(node6, 3);
        tmpchild8 = new IBGNode.IBGChild(node7, 2);

        root.expand(0, tmpchild1);
        tmpchild1.node.expand(0, tmpchild3);
        tmpchild2.node.expand(0, tmpchild5);
        tmpchild4.node.expand(0, tmpchild6);
        tmpchild5.node.expand(0, tmpchild7);

        tmpchild1.next = tmpchild2;
        tmpchild3.next = tmpchild4;
        tmpchild7.next = tmpchild8;

        root.setCost(20);
        node1.setCost(45);
        node2.setCost(50);
        node3.setCost(80);
        node4.setCost(50);
        node5.setCost(65);
        node6.setCost(80);
        node7.setCost(80);

        usedr.set(0);
        usedr.set(3);
        used1.set(0);
        used1.set(1);
        used2.set(1);
        used4.set(1);
        used5.set(2);
        used5.set(3);
    }

    /**
     * Checks that the constructor operates appropriately.
     */
    @Test
    public void testConstructor()
    {
        IndexBitSet ibs = new IndexBitSet();
        IBGNode node    = new IBGNode(ibs, 10);

        assertThat(node.getConfiguration(), is(ibs));
        assertThat(node.getID(), is(10));
        assertThat(node.cost(), lessThan(0.0));
        assertThat(node.isExpanded(), is(false));
        assertThat(node.firstChild(), is(nullValue()));
    }

    /**
     * Checks that the {@link IBGNode#expand} method works correctly.
     */
    @Test
    public void testExpansion()
    {
        assertThat(ibg.rootNode().isExpanded(), is(true));
        assertThat(node1.isExpanded(), is(true));
        assertThat(node2.isExpanded(), is(true));
        assertThat(node3.isExpanded(), is(true));
        assertThat(node4.isExpanded(), is(true));
        assertThat(node5.isExpanded(), is(true));
        assertThat(node6.isExpanded(), is(true));
        assertThat(node7.isExpanded(), is(true));
    }

    /**
     * Checks that the {@link IBGNode#expand} method works correctly.
     */
    @Test
    public void testInternalBitSet()
    {
        assertThat(ibg.rootNode().getConfiguration(), is(rootibs));
        assertThat(node1.getConfiguration(), is(ibs1));
        assertThat(node2.getConfiguration(), is(ibs2));
        assertThat(node3.getConfiguration(), is(ibs3));
        assertThat(node4.getConfiguration(), is(ibs4));
        assertThat(node5.getConfiguration(), is(ibs5));
        assertThat(node6.getConfiguration(), is(ibs6));
        assertThat(node7.getConfiguration(), is(ibs7));
    }

    /**
     */
    @Test
    public void testCostAssignment()
    {
        assertThat(ibg.rootNode().cost(), is(20.0));
        assertThat(node1.cost(), is(45.0));
        assertThat(node2.cost(), is(50.0));
        assertThat(node3.cost(), is(80.0));
        assertThat(node4.cost(), is(50.0));
        assertThat(node5.cost(), is(65.0));
        assertThat(node6.cost(), is(80.0));
        assertThat(node7.cost(), is(80.0));
    }

    /**
     */
    @Test
    public void testStructure()
    {
        assertThat(ibg.rootNode(), is(root));
        assertThat(child1.node, is(tmpchild1.node));
        assertThat(child2.node, is(tmpchild2.node));
        assertThat(child3.node, is(tmpchild3.node));
        assertThat(child4.node, is(tmpchild4.node));
        assertThat(child5.node, is(tmpchild5.node));
        assertThat(child6.node, is(tmpchild6.node));
        assertThat(child7.node, is(tmpchild7.node));
        assertThat(child8.node, is(tmpchild8.node));

        // "leafs"
        assertThat(child3.node.firstChild(), is(nullValue()));
        assertThat(child6.node.firstChild(), is(nullValue()));
        assertThat(child7.node.firstChild(), is(nullValue()));
        assertThat(child8.node.firstChild(), is(nullValue()));
    }

    /**
     */
    @Test
    public void testEdges()
    {
        assertThat(child1.usedIndex, is(3));
        assertThat(child2.usedIndex, is(0));
        assertThat(child3.usedIndex, is(1));
        assertThat(child4.usedIndex, is(0));
        assertThat(child5.usedIndex, is(1));
        assertThat(child6.usedIndex, is(1));
        assertThat(child7.usedIndex, is(3));
        assertThat(child8.usedIndex, is(2));
    }

    /**
     */
    @Test
    public void testUsedAndClearIndexes()
    {
        IndexBitSet ibs = new IndexBitSet();

        root.addUsedIndexes(ibs);
        assertThat(ibs, is(usedr));
        root.clearUsedIndexes(ibs);
        assertThat(ibs, is(empty));

        node1.addUsedIndexes(ibs);
        assertThat(ibs, is(used1));
        node1.clearUsedIndexes(ibs);
        assertThat(ibs, is(empty));

        node2.addUsedIndexes(ibs);
        assertThat(ibs, is(used2));
        node2.clearUsedIndexes(ibs);
        assertThat(ibs, is(empty));

        node3.addUsedIndexes(ibs);
        assertThat(ibs, is(used3));
        node3.clearUsedIndexes(ibs);
        assertThat(ibs, is(empty));

        node4.addUsedIndexes(ibs);
        assertThat(ibs, is(used4));
        node4.clearUsedIndexes(ibs);
        assertThat(ibs, is(empty));

        node5.addUsedIndexes(ibs);
        assertThat(ibs, is(used5));
        node5.clearUsedIndexes(ibs);
        assertThat(ibs, is(empty));

        node6.addUsedIndexes(ibs);
        assertThat(ibs, is(used6));
        node6.clearUsedIndexes(ibs);
        assertThat(ibs, is(empty));

        node7.addUsedIndexes(ibs);
        assertThat(ibs, is(used7));
        node7.clearUsedIndexes(ibs);
        assertThat(ibs, is(empty));
    }

    /**
     */
    @Test
    public void testContainment()
    {
        assertThat(root.usedSetIsSubsetOf(rootibs), is(true));
        assertThat(node1.usedSetIsSubsetOf(rootibs), is(true));
        assertThat(node2.usedSetIsSubsetOf(rootibs), is(true));
        assertThat(node3.usedSetIsSubsetOf(rootibs), is(true));
        assertThat(node4.usedSetIsSubsetOf(rootibs), is(true));
        assertThat(node5.usedSetIsSubsetOf(rootibs), is(true));
        assertThat(node6.usedSetIsSubsetOf(rootibs), is(true));
        assertThat(node7.usedSetIsSubsetOf(rootibs), is(true));

        assertThat(root.usedSetIsSubsetOf(ibs1), is(false));
        assertThat(node1.usedSetIsSubsetOf(ibs1), is(true));
        assertThat(node2.usedSetIsSubsetOf(ibs1), is(true));
        assertThat(node3.usedSetIsSubsetOf(ibs1), is(true));
        assertThat(node4.usedSetIsSubsetOf(ibs1), is(true));
        assertThat(node5.usedSetIsSubsetOf(ibs1), is(false));
        assertThat(node6.usedSetIsSubsetOf(ibs1), is(true));
        assertThat(node7.usedSetIsSubsetOf(ibs1), is(true));

        assertThat(root.usedSetIsSubsetOf(ibs2), is(false));
        assertThat(node1.usedSetIsSubsetOf(ibs2), is(false));
        assertThat(node2.usedSetIsSubsetOf(ibs2), is(true));
        assertThat(node3.usedSetIsSubsetOf(ibs2), is(true));
        assertThat(node4.usedSetIsSubsetOf(ibs2), is(true));
        assertThat(node5.usedSetIsSubsetOf(ibs2), is(true));
        assertThat(node6.usedSetIsSubsetOf(ibs2), is(true));
        assertThat(node7.usedSetIsSubsetOf(ibs2), is(true));

        assertThat(root.usedSetIsSubsetOf(ibs3), is(false));
        assertThat(node1.usedSetIsSubsetOf(ibs3), is(false));
        assertThat(node2.usedSetIsSubsetOf(ibs3), is(false));
        assertThat(node3.usedSetIsSubsetOf(ibs3), is(true));
        assertThat(node4.usedSetIsSubsetOf(ibs3), is(false));
        assertThat(node5.usedSetIsSubsetOf(ibs3), is(false));
        assertThat(node6.usedSetIsSubsetOf(ibs3), is(true));
        assertThat(node7.usedSetIsSubsetOf(ibs3), is(true));

        assertThat(root.usedSetIsSubsetOf(ibs4), is(false));
        assertThat(node1.usedSetIsSubsetOf(ibs4), is(false));
        assertThat(node2.usedSetIsSubsetOf(ibs4), is(true));
        assertThat(node3.usedSetIsSubsetOf(ibs4), is(true));
        assertThat(node4.usedSetIsSubsetOf(ibs4), is(true));
        assertThat(node5.usedSetIsSubsetOf(ibs4), is(false));
        assertThat(node6.usedSetIsSubsetOf(ibs4), is(true));
        assertThat(node7.usedSetIsSubsetOf(ibs4), is(true));

        assertThat(root.usedSetIsSubsetOf(ibs5), is(false));
        assertThat(node1.usedSetIsSubsetOf(ibs5), is(false));
        assertThat(node2.usedSetIsSubsetOf(ibs5), is(false));
        assertThat(node3.usedSetIsSubsetOf(ibs5), is(true));
        assertThat(node4.usedSetIsSubsetOf(ibs5), is(false));
        assertThat(node5.usedSetIsSubsetOf(ibs5), is(true));
        assertThat(node6.usedSetIsSubsetOf(ibs5), is(true));
        assertThat(node7.usedSetIsSubsetOf(ibs5), is(true));

        assertThat(root.usedSetIsSubsetOf(ibs6), is(false));
        assertThat(node1.usedSetIsSubsetOf(ibs6), is(false));
        assertThat(node2.usedSetIsSubsetOf(ibs6), is(false));
        assertThat(node3.usedSetIsSubsetOf(ibs6), is(true));
        assertThat(node4.usedSetIsSubsetOf(ibs6), is(false));
        assertThat(node5.usedSetIsSubsetOf(ibs6), is(false));
        assertThat(node6.usedSetIsSubsetOf(ibs6), is(true));
        assertThat(node7.usedSetIsSubsetOf(ibs6), is(true));

        assertThat(root.usedSetIsSubsetOf(ibs7), is(false));
        assertThat(node1.usedSetIsSubsetOf(ibs7), is(false));
        assertThat(node2.usedSetIsSubsetOf(ibs7), is(false));
        assertThat(node3.usedSetIsSubsetOf(ibs7), is(true));
        assertThat(node4.usedSetIsSubsetOf(ibs7), is(false));
        assertThat(node5.usedSetIsSubsetOf(ibs7), is(false));
        assertThat(node6.usedSetIsSubsetOf(ibs7), is(true));
        assertThat(node7.usedSetIsSubsetOf(ibs7), is(true));

        assertThat(root.usedSetContains(0), is(true));
        assertThat(root.usedSetContains(1), is(false));
        assertThat(root.usedSetContains(2), is(false));
        assertThat(root.usedSetContains(3), is(true));

        assertThat(node1.usedSetContains(0), is(true));
        assertThat(node1.usedSetContains(1), is(true));
        assertThat(node1.usedSetContains(2), is(false));
        assertThat(node1.usedSetContains(3), is(false));

        assertThat(node2.usedSetContains(0), is(false));
        assertThat(node2.usedSetContains(1), is(true));
        assertThat(node2.usedSetContains(2), is(false));
        assertThat(node2.usedSetContains(3), is(false));

        assertThat(node3.usedSetContains(0), is(false));
        assertThat(node3.usedSetContains(1), is(false));
        assertThat(node3.usedSetContains(2), is(false));
        assertThat(node3.usedSetContains(3), is(false));

        assertThat(node4.usedSetContains(0), is(false));
        assertThat(node4.usedSetContains(1), is(true));
        assertThat(node4.usedSetContains(2), is(false));
        assertThat(node4.usedSetContains(3), is(false));

        assertThat(node5.usedSetContains(0), is(false));
        assertThat(node5.usedSetContains(1), is(false));
        assertThat(node5.usedSetContains(2), is(true));
        assertThat(node5.usedSetContains(3), is(true));

        assertThat(node6.usedSetContains(0), is(false));
        assertThat(node6.usedSetContains(1), is(false));
        assertThat(node6.usedSetContains(2), is(false));
        assertThat(node6.usedSetContains(3), is(false));

        assertThat(node7.usedSetContains(0), is(false));
        assertThat(node7.usedSetContains(1), is(false));
        assertThat(node7.usedSetContains(2), is(false));
        assertThat(node7.usedSetContains(3), is(false));
    }
}
