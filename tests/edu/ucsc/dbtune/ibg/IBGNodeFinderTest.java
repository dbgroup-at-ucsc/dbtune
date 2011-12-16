package edu.ucsc.dbtune.ibg;

import edu.ucsc.dbtune.ibg.IndexBenefitGraph.Node;

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
    private static Node root;
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

        a.add(0);

        b.add(1);

        c.add(2);

        d.add(3);
    
        ab.add(0);
        ab.add(1);

        ac.add(0);
        ac.add(2);

        ad.add(0);
        ad.add(3);

        bc.add(1);
        bc.add(2);

        bd.add(1);
        bd.add(3);

        cd.add(2);
        cd.add(3);

        abc.add(0);
        abc.add(1);
        abc.add(2);

        acd.add(0);
        acd.add(2);
        acd.add(3);

        bcd.add(1);
        bcd.add(2);
        bcd.add(3);

        abcd.add(0);
        abcd.add(1);
        abcd.add(2);
        abcd.add(3);
    }

    /**
     */
    @Test
    public void testBasic()
    {
        IBGNodeFinder finder = new IBGNodeFinder();

        IndexBitSet superSet = new IndexBitSet();

        superSet.add(0);
        superSet.add(1);
        superSet.add(2);
        superSet.add(3);
        superSet.add(4);

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
