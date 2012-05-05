package edu.ucsc.dbtune.ibg;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import edu.ucsc.dbtune.ibg.IndexBenefitGraph.Node;
import edu.ucsc.dbtune.metadata.Catalog;
import edu.ucsc.dbtune.metadata.Index;

import org.junit.BeforeClass;
import org.junit.Test;

import static edu.ucsc.dbtune.DBTuneInstances.configureCatalog;
import static edu.ucsc.dbtune.DBTuneInstances.configureIndexBenefitGraph;
import static edu.ucsc.dbtune.DBTuneInstances.configurePowerSet;

import static org.hamcrest.Matchers.anyOf;
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
    private static Map<String, Set<Index>> confs;
    private static Node root;

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
        confs = configurePowerSet(cat);

        IndexBenefitGraph ibg = configureIndexBenefitGraph(confs);

        System.out.println("IBG\n" + ibg);

        root = ibg.rootNode();
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

        assertThat(finder.find(root, confs.get("empty")), is(notNullValue()));
        assertThat(finder.find(root, confs.get("a")), is(notNullValue()));
        assertThat(finder.find(root, confs.get("b")), is(notNullValue()));
        assertThat(finder.find(root, confs.get("c")), is(notNullValue()));
        assertThat(finder.find(root, confs.get("d")), is(notNullValue()));
        assertThat(finder.find(root, confs.get("ab")), is(notNullValue()));
        assertThat(finder.find(root, confs.get("ac")), is(notNullValue()));
        assertThat(finder.find(root, confs.get("ad")), is(notNullValue()));
        assertThat(finder.find(root, confs.get("bc")), is(notNullValue()));
        assertThat(finder.find(root, confs.get("bd")), is(notNullValue()));
        assertThat(finder.find(root, confs.get("cd")), is(notNullValue()));
        assertThat(finder.find(root, confs.get("abc")), is(notNullValue()));
        assertThat(finder.find(root, confs.get("acd")), is(notNullValue()));
        assertThat(finder.find(root, confs.get("bcd")), is(notNullValue()));
        assertThat(finder.find(root, confs.get("abcd")), is(notNullValue()));

        Set<Index> superSet = new HashSet<Index>();

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
                !finder.find(root, confs.get("empty")).getConfiguration().contains(conf.get(0)) ||
                !finder.find(root, confs.get("empty")).getConfiguration().contains(conf.get(2)) ||
                !finder.find(root, confs.get("empty")).getConfiguration().contains(conf.get(3)), 
                is(true));
        assertThat(finder.find(root, confs.get("empty")).cost(), is(80.0));

        assertThat(finder.find(root, confs.get("a")).getConfiguration(), is(confs.get("ac")));
        assertThat(finder.find(root, confs.get("ac")).cost(), is(80.0));

        assertThat(
            finder.find(root, confs.get("b")).getConfiguration(),
            anyOf(is(confs.get("bc")), is(confs.get("bcd"))));
        assertThat(finder.find(root, confs.get("b")).cost(), is(50.0));

        assertThat(finder.find(root, confs.get("ab")).getConfiguration(), is(confs.get("abc")));
        assertThat(finder.find(root, confs.get("ab")).cost(), is(45.0));

        assertThat(finder.find(root, confs.get("ad")).getConfiguration(), is(confs.get("abcd")));
        assertThat(finder.find(root, confs.get("ad")).cost(), is(20.0));

        assertThat(finder.find(root, confs.get("bd")).getConfiguration(), is(confs.get("bcd")));
        assertThat(finder.find(root, confs.get("bd")).cost(), is(50.0));

        assertThat(finder.find(root, confs.get("acd")).getConfiguration(), is(confs.get("abcd")));
        assertThat(finder.find(root, confs.get("acd")).cost(), is(20.0));
    }
}
