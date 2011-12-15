package edu.ucsc.dbtune.ibg;

import edu.ucsc.dbtune.ibg.IndexBenefitGraph.IBGNode;

import edu.ucsc.dbtune.util.IndexBitSet;

import org.junit.BeforeClass;
import org.junit.Test;

import static edu.ucsc.dbtune.DBTuneInstances.configureIndexBenefitGraph;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;

/**
 * Unit test for IBGNodeFinderTest.
 *
 * @author Ivo Jimenez
 */
public class IBGNodeFinderTest
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

        e = new IndexBitSet();
        a = new IndexBitSet();
        b = new IndexBitSet();
        c = new IndexBitSet();
        d = new IndexBitSet();
        ab = new IndexBitSet();
        ac = new IndexBitSet();
        ad = new IndexBitSet();
        bc = new IndexBitSet();
        bd = new IndexBitSet();
        cd = new IndexBitSet();
        abc = new IndexBitSet();
        acd = new IndexBitSet();
        bcd = new IndexBitSet();
        abcd = new IndexBitSet();

        a.set(0);

        b.set(1);

        c.set(2);

        d.set(3);
    
        ab.set(0);
        ab.set(1);

        ac.set(0);
        ac.set(2);

        ad.set(0);
        ad.set(3);

        bc.set(1);
        bc.set(2);

        bd.set(1);
        bd.set(3);

        cd.set(2);
        cd.set(3);

        abc.set(0);
        abc.set(1);
        abc.set(2);

        acd.set(0);
        acd.set(2);
        acd.set(3);

        bcd.set(1);
        bcd.set(2);
        bcd.set(3);

        abcd.set(0);
        abcd.set(1);
        abcd.set(2);
        abcd.set(3);
    }

    /**
     */
    @Test
    public void testBasic()
    {
        IBGNodeFinder finder = new IBGNodeFinder();

        IndexBitSet superSet = new IndexBitSet();

        superSet.set(0);
        superSet.set(1);
        superSet.set(2);
        superSet.set(3);
        superSet.set(4);

        assertThat(finder.find(root, superSet), is(nullValue()));

        // not in the graph
        assertThat(finder.find(root, e), is(nullValue()));
        assertThat(finder.find(root, a), is(nullValue()));
        assertThat(finder.find(root, b), is(nullValue()));
        assertThat(finder.find(root, ab), is(nullValue()));
        assertThat(finder.find(root, ad), is(nullValue()));
        assertThat(finder.find(root, bd), is(nullValue()));
        assertThat(finder.find(root, acd), is(nullValue()));

        assertThat(finder.find(root, c), is(notNullValue()));
        assertThat(finder.find(root, c).getConfiguration(), is(c));
        assertThat(finder.find(root, d), is(notNullValue()));
        assertThat(finder.find(root, d).getConfiguration(), is(d));
        assertThat(finder.find(root, bc), is(notNullValue()));
        assertThat(finder.find(root, bc).getConfiguration(), is(bc));
        assertThat(finder.find(root, cd), is(notNullValue()));
        assertThat(finder.find(root, cd).getConfiguration(), is(cd));
        assertThat(finder.find(root, abc), is(notNullValue()));
        assertThat(finder.find(root, abc).getConfiguration(), is(abc));
        assertThat(finder.find(root, bcd), is(notNullValue()));
        assertThat(finder.find(root, bcd).getConfiguration(), is(bcd));
        assertThat(finder.find(root, abcd), is(notNullValue()));
        assertThat(finder.find(root, abcd).getConfiguration(), is(abcd));
    }
}
