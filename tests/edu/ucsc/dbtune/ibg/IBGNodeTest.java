package edu.ucsc.dbtune.ibg;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.Iterables;

import edu.ucsc.dbtune.metadata.Catalog;
import edu.ucsc.dbtune.metadata.Index;

import org.junit.BeforeClass;
import org.junit.Test;

import static edu.ucsc.dbtune.DBTuneInstances.configureCatalog;
import static edu.ucsc.dbtune.DBTuneInstances.configurePowerSet;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;
import static org.junit.Assert.assertThat;

/**
 * Tests of IBG nodes.
 *
 * @author Ivo Jimenez
 */
public class IBGNodeTest
{
    // CHECKSTYLE:OFF
    public static Catalog cat = configureCatalog();
    private static Map<String, Set<Index>> indexes;
    private static IndexBenefitGraph.Node abcdNode;
    private static IndexBenefitGraph.Node abcNode;
    private static IndexBenefitGraph.Node bcdNode;
    private static IndexBenefitGraph.Node acNode;
    private static IndexBenefitGraph.Node bcNode;
    private static IndexBenefitGraph.Node cdNode;
    private static IndexBenefitGraph.Node cNode;
    private static IndexBenefitGraph.Node dNode;
    private static Set<Index> abcd;
    private static Set<Index> abc;
    private static Set<Index> bcd;
    private static Set<Index> ac;
    private static Set<Index> bc;
    private static Set<Index> cd;
    private static Set<Index> a;
    private static Set<Index> b;
    private static Set<Index> c;
    private static Set<Index> d;

    /**
     * Creates the IBG under test.
     */
    @BeforeClass
    public static void beforeClass() throws Exception
    {
        indexes = configurePowerSet(cat);

        abcd = indexes.get("abcd");
        abc = indexes.get("abc");
        bcd = indexes.get("bcd");
        ac = indexes.get("ac");
        bc = indexes.get("bc");
        cd = indexes.get("cd");
        a = indexes.get("a");
        b = indexes.get("b");
        c = indexes.get("c");
        d = indexes.get("d");

        abcdNode = new IndexBenefitGraph.Node(abcd, 0);
        abcNode = new IndexBenefitGraph.Node(abc, 1);
        bcdNode = new IndexBenefitGraph.Node(bcd, 2);
        acNode = new IndexBenefitGraph.Node(ac, 3);
        bcNode = new IndexBenefitGraph.Node(bc, 4);
        cdNode = new IndexBenefitGraph.Node(cd, 5);
        cNode = new IndexBenefitGraph.Node(c, 6);
        dNode = new IndexBenefitGraph.Node(d, 7);

        abcdNode.setCost(20);
        abcNode.setCost(45);
        bcdNode.setCost(50);
        acNode.setCost(80);
        bcNode.setCost(50);
        cdNode.setCost(65);
        cNode.setCost(80);
        dNode.setCost(80);

        abcdNode.addChild(abcNode, Iterables.get(d, 0));
        abcdNode.addChild(bcdNode, Iterables.get(a, 0));
        abcNode.addChild(acNode, Iterables.get(b, 0));
        abcNode.addChild(bcNode, Iterables.get(a, 0));
        bcdNode.addChild(cdNode, Iterables.get(b, 0));
        bcNode.addChild(cNode, Iterables.get(c, 0));
        cdNode.addChild(dNode, Iterables.get(c, 0));
        cdNode.addChild(cNode, Iterables.get(d, 0));
    }

    /**
     * Checks that the constructor operates appropriately.
     */
    @Test
    public void testConstructor()
    {
        Set<Index> ibs = new HashSet<Index>();
        IndexBenefitGraph.Node node = new IndexBenefitGraph.Node(ibs, 10);

        assertThat(node.getConfiguration(), is(ibs));
        assertThat(node.getId(), is(10));
        assertThat(node.cost(), lessThan(0.0));
        assertThat(node.isExpanded(), is(false));
        assertThat(node.getChildren().isEmpty(), is(true));
        assertThat(node.getEdges().isEmpty(), is(true));
    }

    /**
     * Checks that the {@link IndexBenefitGraph.Node#expand} method works correctly.
     */
    @Test
    public void testExpansion()
    {
        checkExpansion();
    }

    /**
     * Checks that the {@link IndexBenefitGraph.Node#expand} method works correctly.
     */
    @Test
    public void testInternalBitSet()
    {
        checkInternalBitSet();
    }

    /**
     */
    @Test
    public void testCostAssignment()
    {
        checkCostAssignment();
    }

    /**
     */
    @Test
    public void testStructure()
    {
        checkStructure();
    }

    /**
     */
    @Test
    public void testEdges()
    {
        checkEdges();
    }

    public static void checkExpansion()
    {
        assertThat(abcdNode.isExpanded(), is(true));
        assertThat(abcNode.isExpanded(), is(true));
        assertThat(bcdNode.isExpanded(), is(true));
        assertThat(acNode.isExpanded(), is(true));
        assertThat(bcNode.isExpanded(), is(true));
        assertThat(cdNode.isExpanded(), is(true));
        assertThat(cNode.isExpanded(), is(true));
        assertThat(dNode.isExpanded(), is(true));
    }

    public static void checkInternalBitSet()
    {
        assertThat(abcdNode.getConfiguration(), is(abcd));
        assertThat(abcNode.getConfiguration(), is(abc));
        assertThat(bcdNode.getConfiguration(), is(bcd));
        assertThat(acNode.getConfiguration(), is(ac));
        assertThat(bcNode.getConfiguration(), is(bc));
        assertThat(cdNode.getConfiguration(), is(cd));
        assertThat(cNode.getConfiguration(), is(c));
        assertThat(dNode.getConfiguration(), is(d));
    }

    public static void checkCostAssignment()
    {
        assertThat(abcdNode.cost(), is(20.0));
        assertThat(abcNode.cost(), is(45.0));
        assertThat(bcdNode.cost(), is(50.0));
        assertThat(acNode.cost(), is(80.0));
        assertThat(bcNode.cost(), is(50.0));
        assertThat(cdNode.cost(), is(65.0));
        assertThat(cNode.cost(), is(80.0));
        assertThat(dNode.cost(), is(80.0));
    }

    public static void checkStructure()
    {
        assertThat(abcdNode, is(abcdNode));
        assertThat(abcdNode.getChildren().contains(abcdNode), is(false));
        assertThat(abcdNode.getChildren().contains(abcNode), is(true));
        assertThat(abcdNode.getChildren().contains(bcdNode), is(true));
        assertThat(abcdNode.getChildren().contains(acNode), is(false));
        assertThat(abcdNode.getChildren().contains(bcNode), is(false));
        assertThat(abcdNode.getChildren().contains(cdNode), is(false));
        assertThat(abcdNode.getChildren().contains(cNode), is(false));
        assertThat(abcdNode.getChildren().contains(dNode), is(false));

        assertThat(abcNode.getChildren().contains(abcdNode), is(false));
        assertThat(abcNode.getChildren().contains(bcdNode), is(false));
        assertThat(abcNode.getChildren().contains(acNode), is(true));
        assertThat(abcNode.getChildren().contains(bcNode), is(true));
        assertThat(abcNode.getChildren().contains(cdNode), is(false));
        assertThat(abcNode.getChildren().contains(cNode), is(false));
        assertThat(abcNode.getChildren().contains(dNode), is(false));

        assertThat(bcdNode.getChildren().contains(abcdNode), is(false));
        assertThat(bcdNode.getChildren().contains(bcdNode), is(false));
        assertThat(bcdNode.getChildren().contains(acNode), is(false));
        assertThat(bcdNode.getChildren().contains(bcNode), is(false));
        assertThat(bcdNode.getChildren().contains(cdNode), is(true));
        assertThat(bcdNode.getChildren().contains(cNode), is(false));
        assertThat(bcdNode.getChildren().contains(dNode), is(false));

        // "leafs"
        assertThat(acNode.getChildren().isEmpty(), is(true));
        assertThat(cNode.getChildren().isEmpty(), is(true));
        assertThat(dNode.getChildren().isEmpty(), is(true));
    }

    public static void checkEdges()
    {
        assertThat(abcdNode, is(abcdNode));
        assertThat(abcdNode.getEdges().size(), is(2));
        assertThat(d.contains(abcdNode.getEdges().get(0).getUsedIndex()), is(true));
        assertThat(a.contains(abcdNode.getEdges().get(1).getUsedIndex()), is(true));
        assertThat(abcNode.getEdges().size(), is(2));
        assertThat(b.contains(abcNode.getEdges().get(0).getUsedIndex()), is(true));
        assertThat(a.contains(abcNode.getEdges().get(1).getUsedIndex()), is(true));
        assertThat(bcdNode.getEdges().size(), is(1));
        assertThat(b.contains(bcdNode.getEdges().get(0).getUsedIndex()), is(true));
        assertThat(acNode.getEdges().size(), is(0));
        assertThat(bcNode.getEdges().size(), is(1));
        assertThat(c.contains(bcNode.getEdges().get(0).getUsedIndex()), is(true));
        assertThat(cdNode.getEdges().size(), is(2));
        assertThat(c.contains(cdNode.getEdges().get(0).getUsedIndex()), is(true));
        assertThat(d.contains(cdNode.getEdges().get(1).getUsedIndex()), is(true));
        assertThat(cNode.getEdges().size(), is(0));
        assertThat(dNode.getEdges().size(), is(0));
    }
    // CHECKSTYLE:ON
}
