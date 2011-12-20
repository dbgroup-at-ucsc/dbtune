package edu.ucsc.dbtune.util;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import edu.ucsc.dbtune.metadata.Catalog;
import edu.ucsc.dbtune.metadata.Index;

import org.junit.BeforeClass;
import org.junit.Test;

import static edu.ucsc.dbtune.DBTuneInstances.configureCatalog;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

/**
 * Tests of IBG nodes.
 *
 * @author Ivo Jimenez
 */
public class IndexBitSetTest
{
    private static Catalog cat;

    /**
     * configures the {@link IndexBenefitGraph} under test.
     * @throws Exception
     *      if something goes wrong
     */
    @BeforeClass
    public static void beforeClass() throws Exception
    {
        cat = configureCatalog();
    }

    /**
     * Test that an index bitset is constructed correctly.
     * 
     * @throws Exception
     *     if the catalog can't be configured properly.
     */
    @Test
    public void testBasicUsage() throws Exception
    {
        List<Index> conf = cat.schemas().get(0).indexes();
        BitArraySet<Index> bs = new BitArraySet<Index>();

        for (Index idx : conf)
            bs.add(idx);

        for (Index idx : conf)
            assertThat(bs.contains(idx), is(true));

        BitArraySet<Index> other = new BitArraySet<Index>(bs);

        assertThat(bs, is(other));

        other = new BitArraySet<Index>();

        other.add(conf.get(1));
        other.add(conf.get(3));
        other.add(conf.get(5));
        other.add(conf.get(7));

        assertThat(bs.containsAll(other), is(true));

        bs = new BitArraySet<Index>();

        bs.addAll(other);

        assertThat(bs, is(other));
    }

    /**
     * @throws Exception
     *      if the catalog can't be configured
     */
    @Test
    public void testAgainstOtherSetImplementation() throws Exception
    {
        List<Index> conf = cat.schemas().get(0).indexes();
        Set<Index> conf1 = new HashSet<Index>(conf);
        Set<Index> conf2 = new BitArraySet<Index>(conf);

        for (Index idx : conf) {
            assertThat(conf1.contains(idx), is(true));
        }

        for (Index idx : conf) {
            assertThat(conf2.contains(idx), is(true));
        }

        //assertThat(conf1, is(conf2));
    }

    /**
     * @throws Exception
     *      if the catalog can't be configured
     */
    @Test
    public void testRemainingSetOperations() throws Exception
    {
    }
}
