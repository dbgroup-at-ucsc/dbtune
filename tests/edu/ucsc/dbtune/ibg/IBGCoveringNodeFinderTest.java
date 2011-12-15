package edu.ucsc.dbtune.ibg;

import java.util.List;

import edu.ucsc.dbtune.ibg.IndexBenefitGraph.IBGNode;
import edu.ucsc.dbtune.util.IndexBitSet;

import org.junit.BeforeClass;
import org.junit.Test;

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
    private static IBGNode root;
    private static IndexBitSet e;
    private static IndexBitSet a;
    private static IndexBitSet b;
    private static IndexBitSet c;
    private static IndexBitSet d;
    private static IndexBitSet ab;
    private static IndexBitSet ac;
    private static IndexBitSet ad;
    private static IndexBitSet bc;
    private static IndexBitSet bd;
    private static IndexBitSet cd;
    private static IndexBitSet abc;
    private static IndexBitSet acd;
    private static IndexBitSet bcd;
    private static IndexBitSet abcd;

    /**
     * Setup for the test.
     */
    @BeforeClass
    public static void beforeClass()
    {
        root = configureIndexBenefitGraph().rootNode();

        List<IndexBitSet> list = configurePowerSet();

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
     */
    @Test
    public void testThatAllAreCovered()
    {
        IBGCoveringNodeFinder finder = new IBGCoveringNodeFinder();

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

        IndexBitSet superSet = new IndexBitSet();

        superSet.add(0);
        superSet.add(1);
        superSet.add(2);
        superSet.add(3);
        superSet.add(4);

        assertThat(finder.find(root, superSet), is(nullValue()));
    }

    /**
     */
    @Test
    public void testCoveringIsCorrect()
    {
        IBGCoveringNodeFinder finder = new IBGCoveringNodeFinder();

        assertThat(
                !finder.find(root, e).getConfiguration().contains(0) ||
                !finder.find(root, e).getConfiguration().contains(2) ||
                !finder.find(root, e).getConfiguration().contains(3), is(true));
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
        // XXX: do it as part of interactions
    }
}
