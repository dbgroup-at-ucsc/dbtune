package edu.ucsc.dbtune.ibg;

import java.util.List;

import edu.ucsc.dbtune.metadata.Catalog;
import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.util.BitArraySet;

import org.junit.BeforeClass;
import org.junit.Test;

import static edu.ucsc.dbtune.DBTuneInstances.configureCatalog;
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
    private static IndexBenefitGraph.Node root;
    private static IndexBenefitGraph.Node node1;
    private static IndexBenefitGraph.Node node2;
    private static IndexBenefitGraph.Node node3;
    private static IndexBenefitGraph.Node node4;
    private static IndexBenefitGraph.Node node5;
    private static IndexBenefitGraph.Node node6;
    private static IndexBenefitGraph.Node node7;
    private static IndexBenefitGraph.Node.Child child1;
    private static IndexBenefitGraph.Node.Child child2;
    private static IndexBenefitGraph.Node.Child child3;
    private static IndexBenefitGraph.Node.Child child4;
    private static IndexBenefitGraph.Node.Child child5;
    private static IndexBenefitGraph.Node.Child child6;
    private static IndexBenefitGraph.Node.Child child7;
    private static IndexBenefitGraph.Node.Child child8;
    private static IndexBenefitGraph.Node.Child tmpchild1;
    private static IndexBenefitGraph.Node.Child tmpchild2;
    private static IndexBenefitGraph.Node.Child tmpchild3;
    private static IndexBenefitGraph.Node.Child tmpchild4;
    private static IndexBenefitGraph.Node.Child tmpchild5;
    private static IndexBenefitGraph.Node.Child tmpchild6;
    private static IndexBenefitGraph.Node.Child tmpchild7;
    private static IndexBenefitGraph.Node.Child tmpchild8;
    private static BitArraySet<Index> empty;
    private static BitArraySet<Index> rootibs;
    private static BitArraySet<Index> ibs1;
    private static BitArraySet<Index> ibs2;
    private static BitArraySet<Index> ibs3;
    private static BitArraySet<Index> ibs4;
    private static BitArraySet<Index> ibs5;
    private static BitArraySet<Index> ibs6;
    private static BitArraySet<Index> ibs7;
    private static BitArraySet<Index> usedr;
    private static BitArraySet<Index> used1;
    private static BitArraySet<Index> used2;
    private static BitArraySet<Index> used3;
    private static BitArraySet<Index> used4;
    private static BitArraySet<Index> used5;
    private static BitArraySet<Index> used6;
    private static BitArraySet<Index> used7;
    private static List<Index> indexes;

    /**
     * Creates the IBG under test.
     */
    @BeforeClass
    public static void beforeClass() throws Exception
    {
        Catalog cat = configureCatalog();
        indexes = cat.schemas().get(0).indexes();
        empty = new BitArraySet<Index>();
        rootibs = new BitArraySet<Index>();
        ibs1 = new BitArraySet<Index>();
        ibs2 = new BitArraySet<Index>();
        ibs3 = new BitArraySet<Index>();
        ibs4 = new BitArraySet<Index>();
        ibs5 = new BitArraySet<Index>();
        ibs6 = new BitArraySet<Index>();
        ibs7 = new BitArraySet<Index>();

        rootibs.add(indexes.get(0));
        rootibs.add(indexes.get(1));
        rootibs.add(indexes.get(2));
        rootibs.add(indexes.get(3));

        ibs1.add(indexes.get(0));
        ibs1.add(indexes.get(1));
        ibs1.add(indexes.get(2));

        ibs2.add(indexes.get(1));
        ibs2.add(indexes.get(2));
        ibs2.add(indexes.get(3));

        ibs3.add(indexes.get(0));
        ibs3.add(indexes.get(2));

        ibs4.add(indexes.get(1));
        ibs4.add(indexes.get(2));

        ibs5.add(indexes.get(2));
        ibs5.add(indexes.get(3));

        ibs6.add(indexes.get(2));

        ibs7.add(indexes.get(3));

        ibg = configureIndexBenefitGraph(cat);

        root = new IndexBenefitGraph.Node(rootibs, 0);
        node1 = new IndexBenefitGraph.Node(ibs1, 1);
        node2 = new IndexBenefitGraph.Node(ibs2, 2);
        node3 = new IndexBenefitGraph.Node(ibs3, 3);
        node4 = new IndexBenefitGraph.Node(ibs4, 4);
        node5 = new IndexBenefitGraph.Node(ibs5, 5);
        node6 = new IndexBenefitGraph.Node(ibs6, 6);
        node7 = new IndexBenefitGraph.Node(ibs7, 7);

        child1 = ibg.rootNode().firstChild();
        child2 = child1.getNext();
        child3 = child1.getNode().firstChild();
        child4 = child3.getNext();
        child5 = child2.getNode().firstChild();
        child6 = child4.getNode().firstChild();
        child7 = child5.getNode().firstChild();
        child8 = child7.getNext();

        tmpchild1 = new IndexBenefitGraph.Node.Child(node1, indexes.get(3));
        tmpchild2 = new IndexBenefitGraph.Node.Child(node2, indexes.get(0));
        tmpchild3 = new IndexBenefitGraph.Node.Child(node3, indexes.get(1));
        tmpchild4 = new IndexBenefitGraph.Node.Child(node4, indexes.get(0));
        tmpchild5 = new IndexBenefitGraph.Node.Child(node5, indexes.get(1));
        tmpchild6 = new IndexBenefitGraph.Node.Child(node6, indexes.get(1));
        tmpchild7 = new IndexBenefitGraph.Node.Child(node6, indexes.get(3));
        tmpchild8 = new IndexBenefitGraph.Node.Child(node7, indexes.get(2));

        root.expand(0, tmpchild1);
        tmpchild1.getNode().expand(0, tmpchild3);
        tmpchild2.getNode().expand(0, tmpchild5);
        tmpchild4.getNode().expand(0, tmpchild6);
        tmpchild5.getNode().expand(0, tmpchild7);

        tmpchild1.setNext(tmpchild2);
        tmpchild3.setNext(tmpchild4);
        tmpchild7.setNext(tmpchild8);

        root.setCost(20);
        node1.setCost(45);
        node2.setCost(50);
        node3.setCost(80);
        node4.setCost(50);
        node5.setCost(65);
        node6.setCost(80);
        node7.setCost(80);

        usedr = new BitArraySet<Index>();
        used1 = new BitArraySet<Index>();
        used2 = new BitArraySet<Index>();
        used3 = new BitArraySet<Index>();
        used4 = new BitArraySet<Index>();
        used5 = new BitArraySet<Index>();
        used6 = new BitArraySet<Index>();
        used7 = new BitArraySet<Index>();

        usedr.add(indexes.get(0));
        usedr.add(indexes.get(3));
        used1.add(indexes.get(0));
        used1.add(indexes.get(1));
        used2.add(indexes.get(1));
        used4.add(indexes.get(1));
        used5.add(indexes.get(2));
        used5.add(indexes.get(3));
    }

    /**
     * Checks that the constructor operates appropriately.
     */
    @Test
    public void testConstructor()
    {
        BitArraySet<Index> ibs = new BitArraySet<Index>();
        IndexBenefitGraph.Node node = new IndexBenefitGraph.Node(ibs, 10);

        assertThat(node.getConfiguration(), is(ibs));
        assertThat(node.getId(), is(10));
        assertThat(node.cost(), lessThan(0.0));
        assertThat(node.isExpanded(), is(false));
        assertThat(node.firstChild(), is(nullValue()));
    }

    /**
     * Checks that the {@link IndexBenefitGraph.Node#expand} method works correctly.
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
     * Checks that the {@link IndexBenefitGraph.Node#expand} method works correctly.
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
        assertThat(child1.getNode(), is(tmpchild1.getNode()));
        assertThat(child2.getNode(), is(tmpchild2.getNode()));
        assertThat(child3.getNode(), is(tmpchild3.getNode()));
        assertThat(child4.getNode(), is(tmpchild4.getNode()));
        assertThat(child5.getNode(), is(tmpchild5.getNode()));
        assertThat(child6.getNode(), is(tmpchild6.getNode()));
        assertThat(child7.getNode(), is(tmpchild7.getNode()));
        assertThat(child8.getNode(), is(tmpchild8.getNode()));

        // "leafs"
        assertThat(child3.getNode().firstChild(), is(nullValue()));
        assertThat(child6.getNode().firstChild(), is(nullValue()));
        assertThat(child7.getNode().firstChild(), is(nullValue()));
        assertThat(child8.getNode().firstChild(), is(nullValue()));
    }

    /**
     */
    @Test
    public void testEdges()
    {
        assertThat(child1.getUsedIndex(), is(indexes.get(3)));
        assertThat(child2.getUsedIndex(), is(indexes.get(0)));
        assertThat(child3.getUsedIndex(), is(indexes.get(1)));
        assertThat(child4.getUsedIndex(), is(indexes.get(0)));
        assertThat(child5.getUsedIndex(), is(indexes.get(1)));
        assertThat(child6.getUsedIndex(), is(indexes.get(1)));
        assertThat(child7.getUsedIndex(), is(indexes.get(3)));
        assertThat(child8.getUsedIndex(), is(indexes.get(2)));
    }

    /**
     */
    @Test
    public void testUsedAndClearIndexes()
    {
        BitArraySet<Index> ibs = new BitArraySet<Index>();

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

        assertThat(root.usedSetContains(indexes.get(0)), is(true));
        assertThat(root.usedSetContains(indexes.get(1)), is(false));
        assertThat(root.usedSetContains(indexes.get(2)), is(false));
        assertThat(root.usedSetContains(indexes.get(3)), is(true));

        assertThat(node1.usedSetContains(indexes.get(0)), is(true));
        assertThat(node1.usedSetContains(indexes.get(1)), is(true));
        assertThat(node1.usedSetContains(indexes.get(2)), is(false));
        assertThat(node1.usedSetContains(indexes.get(3)), is(false));

        assertThat(node2.usedSetContains(indexes.get(0)), is(false));
        assertThat(node2.usedSetContains(indexes.get(1)), is(true));
        assertThat(node2.usedSetContains(indexes.get(2)), is(false));
        assertThat(node2.usedSetContains(indexes.get(3)), is(false));

        assertThat(node3.usedSetContains(indexes.get(0)), is(false));
        assertThat(node3.usedSetContains(indexes.get(1)), is(false));
        assertThat(node3.usedSetContains(indexes.get(2)), is(false));
        assertThat(node3.usedSetContains(indexes.get(3)), is(false));

        assertThat(node4.usedSetContains(indexes.get(0)), is(false));
        assertThat(node4.usedSetContains(indexes.get(1)), is(true));
        assertThat(node4.usedSetContains(indexes.get(2)), is(false));
        assertThat(node4.usedSetContains(indexes.get(3)), is(false));

        assertThat(node5.usedSetContains(indexes.get(0)), is(false));
        assertThat(node5.usedSetContains(indexes.get(1)), is(false));
        assertThat(node5.usedSetContains(indexes.get(2)), is(true));
        assertThat(node5.usedSetContains(indexes.get(3)), is(true));

        assertThat(node6.usedSetContains(indexes.get(0)), is(false));
        assertThat(node6.usedSetContains(indexes.get(1)), is(false));
        assertThat(node6.usedSetContains(indexes.get(2)), is(false));
        assertThat(node6.usedSetContains(indexes.get(3)), is(false));

        assertThat(node7.usedSetContains(indexes.get(0)), is(false));
        assertThat(node7.usedSetContains(indexes.get(1)), is(false));
        assertThat(node7.usedSetContains(indexes.get(2)), is(false));
        assertThat(node7.usedSetContains(indexes.get(3)), is(false));
    }
    // CHECKSTYLE:ON
}
