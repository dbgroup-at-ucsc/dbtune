package edu.ucsc.dbtune.ibg;

import java.util.List;
import java.util.Set;

import edu.ucsc.dbtune.ibg.IndexBenefitGraph.Node;
import edu.ucsc.dbtune.metadata.Catalog;
import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.util.BitArraySet;

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
    private static Set<Index> e;
    private static Set<Index> a;
    private static Set<Index> b;
    private static Set<Index> c;
    private static Set<Index> d;
    private static Set<Index> ab;
    private static Set<Index> ac;
    private static Set<Index> ad;
    private static Set<Index> bc;
    private static Set<Index> bd;
    private static Set<Index> cd;
    private static Set<Index> abc;
    private static Set<Index> acd;
    private static Set<Index> bcd;
    private static Set<Index> abcd;

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

        e = new BitArraySet<Index>();
        a = new BitArraySet<Index>();
        b = new BitArraySet<Index>();
        c = new BitArraySet<Index>();
        d = new BitArraySet<Index>();
        ab = new BitArraySet<Index>();
        ac = new BitArraySet<Index>();
        ad = new BitArraySet<Index>();
        bc = new BitArraySet<Index>();
        bd = new BitArraySet<Index>();
        cd = new BitArraySet<Index>();
        abc = new BitArraySet<Index>();
        acd = new BitArraySet<Index>();
        bcd = new BitArraySet<Index>();
        abcd = new BitArraySet<Index>();

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

        BitArraySet<Index> superSet = new BitArraySet<Index>();

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
