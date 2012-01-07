package edu.ucsc.dbtune.ibg;

import java.util.List;
import java.util.Map;
import java.util.Set;

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
        root = configureIndexBenefitGraph(cat).rootNode();
        confs = configurePowerSet(cat);
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
                !finder.find(root, confs.get("empty")).getConfiguration().contains(conf.get(0)) ||
                !finder.find(root, confs.get("empty")).getConfiguration().contains(conf.get(2)) ||
                !finder.find(root, confs.get("empty")).getConfiguration().contains(conf.get(3)), 
                is(true));
        assertThat(finder.find(root, confs.get("empty")).cost(), is(80.0));

        assertThat(finder.find(root, confs.get("a")).getConfiguration(), is(confs.get("ac")));
        assertThat(finder.find(root, confs.get("ac")).cost(), is(80.0));

        assertThat(finder.find(root, confs.get("b")).getConfiguration(), is(confs.get("bc")));
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

    /**
     *
     */
    @Test
    public void testFindFast()
    {
        IBGCoveringNodeFinder finder = new IBGCoveringNodeFinder();

        // check a bad guess
        assertThat(
            finder.findFast(root, confs.get("a"),
                finder.find(root, confs.get("cd"))).getConfiguration(),
            is(confs.get("ac")));

        // check good guesses
        assertThat(
                finder.findFast(root, confs.get("c"),
                    finder.find(root, confs.get("cd"))).getConfiguration(),
                is(confs.get("c")));

        assertThat(
                finder.find(root, confs.get("empty")),
                is(finder.findFast(root, confs.get("empty"), null)));
        assertThat(
                finder.find(root, confs.get("a")), is(finder.findFast(root, confs.get("a"), null)));
        assertThat(
                finder.find(root, confs.get("b")), is(finder.findFast(root, confs.get("b"), null)));
        assertThat(
                finder.find(root, confs.get("c")), is(finder.findFast(root, confs.get("c"), null)));
        assertThat(
                finder.find(root, confs.get("d")), is(finder.findFast(root, confs.get("d"), null)));
        assertThat(
                finder.find(root, confs.get("ab")), is(finder.findFast(root, confs.get("ab"),  
                        null)));
        assertThat(
                finder.find(root, confs.get("ac")), is(finder.findFast(root, confs.get("ac"),  
                        null)));
        assertThat(
                finder.find(root, confs.get("ad")), is(finder.findFast(root, confs.get("ad"),  
                        null)));
        assertThat(
                finder.find(root, confs.get("bc")), is(finder.findFast(root, confs.get("bc"),  
                        null)));
        assertThat(
                finder.find(root, confs.get("bd")), is(finder.findFast(root, confs.get("bd"),  
                        null)));
        assertThat(
                finder.find(root, confs.get("cd")), is(finder.findFast(root, confs.get("cd"),  
                        null)));
        assertThat(
                finder.find(root, confs.get("abc")), is(finder.findFast(root, confs.get("abc"), 
                        null)));
        assertThat(
                finder.find(root, confs.get("acd")), is(finder.findFast(root, confs.get("acd"), 
                        null)));
        assertThat(
                finder.find(root, confs.get("bcd")), is(finder.findFast(root, confs.get("bcd"), 
                        null)));
        assertThat(
                finder.find(root, confs.get("abcd")), is(finder.findFast(root, confs.get("abcd"), 
                        null)));
    }
}
