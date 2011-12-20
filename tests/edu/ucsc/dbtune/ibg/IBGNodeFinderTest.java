package edu.ucsc.dbtune.ibg;

import java.util.List;

import edu.ucsc.dbtune.ibg.IndexBenefitGraph.Node;
import edu.ucsc.dbtune.metadata.Catalog;
import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.util.IndexBitSet;

import org.junit.BeforeClass;
import org.junit.Test;

import static edu.ucsc.dbtune.DBTuneInstances.configureCatalog;
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
    private static IndexBitSet<Index> e;
    private static IndexBitSet<Index> a;
    private static IndexBitSet<Index> b;
    private static IndexBitSet<Index> c;
    private static IndexBitSet<Index> d;
    private static IndexBitSet<Index> ab;
    private static IndexBitSet<Index> ac;
    private static IndexBitSet<Index> ad;
    private static IndexBitSet<Index> bc;
    private static IndexBitSet<Index> bd;
    private static IndexBitSet<Index> cd;
    private static IndexBitSet<Index> abc;
    private static IndexBitSet<Index> acd;
    private static IndexBitSet<Index> bcd;
    private static IndexBitSet<Index> abcd;

    /**
     * Setup for the test.
     *
     * @throws Exception
     *      if something goes wrong
     */
    @BeforeClass
    public static void beforeClass() throws Exception
    {
        Catalog cat = configureCatalog();
        List<Index> indexes = cat.schemas().get(0).indexes();
        root = configureIndexBenefitGraph(cat).rootNode();

        e = new IndexBitSet<Index>();
        a = new IndexBitSet<Index>();
        b = new IndexBitSet<Index>();
        c = new IndexBitSet<Index>();
        d = new IndexBitSet<Index>();
        ab = new IndexBitSet<Index>();
        ac = new IndexBitSet<Index>();
        ad = new IndexBitSet<Index>();
        bc = new IndexBitSet<Index>();
        bd = new IndexBitSet<Index>();
        cd = new IndexBitSet<Index>();
        abc = new IndexBitSet<Index>();
        acd = new IndexBitSet<Index>();
        bcd = new IndexBitSet<Index>();
        abcd = new IndexBitSet<Index>();

        a.add(indexes.get(0));

        b.add(indexes.get(1));

        c.add(indexes.get(2));

        d.add(indexes.get(3));
    
        ab.add(indexes.get(0));
        ab.add(indexes.get(1));

        ac.add(indexes.get(0));
        ac.add(indexes.get(2));

        ad.add(indexes.get(0));
        ad.add(indexes.get(3));

        bc.add(indexes.get(1));
        bc.add(indexes.get(2));

        bd.add(indexes.get(1));
        bd.add(indexes.get(3));

        cd.add(indexes.get(2));
        cd.add(indexes.get(3));

        abc.add(indexes.get(0));
        abc.add(indexes.get(1));
        abc.add(indexes.get(2));

        acd.add(indexes.get(0));
        acd.add(indexes.get(2));
        acd.add(indexes.get(3));

        bcd.add(indexes.get(1));
        bcd.add(indexes.get(2));
        bcd.add(indexes.get(3));

        abcd.add(indexes.get(0));
        abcd.add(indexes.get(1));
        abcd.add(indexes.get(2));
        abcd.add(indexes.get(3));
    }

    /**
     * @throws Exception
     *      if something goes wrong
     */
    @Test
    public void testBasic() throws Exception
    {
        IBGNodeFinder finder = new IBGNodeFinder();

        List<Index> conf = configureCatalog().schemas().get(0).indexes();

        IndexBitSet<Index> superSet = new IndexBitSet<Index>();

        superSet.add(conf.get(0));
        superSet.add(conf.get(1));
        superSet.add(conf.get(2));
        superSet.add(conf.get(3));
        superSet.add(conf.get(4));

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
