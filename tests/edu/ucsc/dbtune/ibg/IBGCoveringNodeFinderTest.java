package edu.ucsc.dbtune.ibg;

import java.util.List;

import edu.ucsc.dbtune.ibg.IndexBenefitGraph.Node;
import edu.ucsc.dbtune.metadata.Catalog;
import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.util.BitArraySet;

import org.junit.BeforeClass;
import org.junit.Test;

import static edu.ucsc.dbtune.DBTuneInstances.configureCatalog;
import static edu.ucsc.dbtune.DBTuneInstances.configureIndexBenefitGraph;
import static edu.ucsc.dbtune.DBTuneInstances.configurePowerSet;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;

/**
 * Unit test for IBGCoveringNodeFinderTest.
 *
 * @author Ivo Jimenez
 */
public class IBGCoveringNodeFinderTest
{
    private static Catalog cat;
    private static Node root;
    private static BitArraySet<Index> e;
    private static BitArraySet<Index> a;
    private static BitArraySet<Index> b;
    private static BitArraySet<Index> c;
    private static BitArraySet<Index> d;
    private static BitArraySet<Index> ab;
    private static BitArraySet<Index> ac;
    private static BitArraySet<Index> ad;
    private static BitArraySet<Index> bc;
    private static BitArraySet<Index> bd;
    private static BitArraySet<Index> cd;
    private static BitArraySet<Index> abc;
    private static BitArraySet<Index> acd;
    private static BitArraySet<Index> bcd;
    private static BitArraySet<Index> abcd;

    /**
     * Setup for the test.
     *
     * @throws Exception
     *      if something goes wrong
     */
    @BeforeClass
    public static void beforeClass() throws Exception
    {
        cat = configureCatalog();
        root = configureIndexBenefitGraph(cat).rootNode();

        List<BitArraySet<Index>> list = configurePowerSet(cat);

        e = list.get(0);
        a = list.get(1);
        b = list.get(2);
        c = list.get(3);
        d = list.get(4);
        ab = list.get(5);
        ac = list.get(6);
        ad = list.get(7);
        bc = list.get(8);
        bd = list.get(9);
        cd = list.get(10);
        abc = list.get(11);
        acd = list.get(12);
        bcd = list.get(13);
        abcd = list.get(14);
    }

    /**
     *
     * @throws Exception
     *      if something goes wrong
     */
    @Test
    public void testThatAllAreCovered() throws Exception
    {
        IBGCoveringNodeFinder finder = new IBGCoveringNodeFinder();

        List<Index> conf = cat.schemas().get(0).indexes();

        assertThat(finder.find(root, e), is(notNullValue()));
        assertThat(finder.find(root, a), is(notNullValue()));
        assertThat(finder.find(root, b), is(notNullValue()));
        assertThat(finder.find(root, c), is(notNullValue()));
        assertThat(finder.find(root, d), is(notNullValue()));
        assertThat(finder.find(root, ab), is(notNullValue()));
        assertThat(finder.find(root, ac), is(notNullValue()));
        assertThat(finder.find(root, ad), is(notNullValue()));
        assertThat(finder.find(root, bc), is(notNullValue()));
        assertThat(finder.find(root, bd), is(notNullValue()));
        assertThat(finder.find(root, cd), is(notNullValue()));
        assertThat(finder.find(root, abc), is(notNullValue()));
        assertThat(finder.find(root, acd), is(notNullValue()));
        assertThat(finder.find(root, bcd), is(notNullValue()));
        assertThat(finder.find(root, abcd), is(notNullValue()));

        BitArraySet<Index> superSet = new BitArraySet<Index>();

        superSet.add(conf.get(0));
        superSet.add(conf.get(1));
        superSet.add(conf.get(2));
        superSet.add(conf.get(3));
        superSet.add(conf.get(4));

        assertThat(finder.find(root, superSet), is(nullValue()));
    }

    /**
     */
    @Test
    public void testCoveringIsCorrect()
    {
        IBGCoveringNodeFinder finder = new IBGCoveringNodeFinder();
        List<Index> conf = cat.schemas().get(0).indexes();

        assertThat(
                !finder.find(root, e).getConfiguration().contains(conf.get(0)) ||
                !finder.find(root, e).getConfiguration().contains(conf.get(2)) ||
                !finder.find(root, e).getConfiguration().contains(conf.get(3)), is(true));
        assertThat(finder.find(root, e).cost(), is(80.0));

        assertThat(finder.find(root, a).getConfiguration(), is(ac));
        assertThat(finder.find(root, ac).cost(), is(80.0));

        assertThat(finder.find(root, b).getConfiguration(), is(bc));
        assertThat(finder.find(root, b).cost(), is(50.0));

        assertThat(finder.find(root, ab).getConfiguration(), is(abc));
        assertThat(finder.find(root, ab).cost(), is(45.0));

        assertThat(finder.find(root, ad).getConfiguration(), is(abcd));
        assertThat(finder.find(root, ad).cost(), is(20.0));

        assertThat(finder.find(root, bd).getConfiguration(), is(bcd));
        assertThat(finder.find(root, bd).cost(), is(50.0));

        assertThat(finder.find(root, acd).getConfiguration(), is(abcd));
        assertThat(finder.find(root, acd).cost(), is(20.0));
    }

    /**
     *
     */
    @Test
    public void testFindFast()
    {
        IBGCoveringNodeFinder finder = new IBGCoveringNodeFinder();

        // check that a bad guess
        assertThat(finder.findFast(root, a, finder.find(root, cd)).getConfiguration(), is(ac));

        // check that a good guess
        assertThat(finder.findFast(root, c, finder.find(root, cd)).getConfiguration(), is(c));

        assertThat(finder.find(root, e), is(finder.findFast(root, e, null)));
        assertThat(finder.find(root, a), is(finder.findFast(root, a, null)));
        assertThat(finder.find(root, b), is(finder.findFast(root, b, null)));
        assertThat(finder.find(root, c), is(finder.findFast(root, c, null)));
        assertThat(finder.find(root, d), is(finder.findFast(root, d, null)));
        assertThat(finder.find(root, ab), is(finder.findFast(root, ab, null)));
        assertThat(finder.find(root, ac), is(finder.findFast(root, ac, null)));
        assertThat(finder.find(root, ad), is(finder.findFast(root, ad, null)));
        assertThat(finder.find(root, bc), is(finder.findFast(root, bc, null)));
        assertThat(finder.find(root, bd), is(finder.findFast(root, bd, null)));
        assertThat(finder.find(root, cd), is(finder.findFast(root, cd, null)));
        assertThat(finder.find(root, abc), is(finder.findFast(root, abc, null)));
        assertThat(finder.find(root, acd), is(finder.findFast(root, acd, null)));
        assertThat(finder.find(root, bcd), is(finder.findFast(root, bcd, null)));
        assertThat(finder.find(root, abcd), is(finder.findFast(root, abcd, null)));
    }
}
