package edu.ucsc.dbtune.metadata;

import edu.ucsc.dbtune.util.IndexBitSet;

import org.junit.BeforeClass;
import org.junit.Test;

import static edu.ucsc.dbtune.DBTuneInstances.configureCatalog;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

/**
 * Test for the Configuration class.
 *
 * @author Ivo Jimenez
 */
public class ConfigurationBitSetTest
{
    /** all the indexes in the catalog. */
    private static Configuration allIndexes = new Configuration("all");

    /**
     * Checks that the basic usage works.
     */
    @Test
    public void basicUsage()
    {
        IndexBitSet bitSet = new IndexBitSet();

        bitSet.set(0);
        bitSet.set(allIndexes.size() - 2);

        ConfigurationBitSet conf1 = new ConfigurationBitSet(allIndexes, bitSet);

        assertThat(conf1.contains(allIndexes.getIndexAt(0)), is(true));
        assertThat(conf1.contains(allIndexes.getIndexAt(allIndexes.size() - 2)), is(true));
    }

    /**
     * Test that a configuration bitset is comparable to any other configuration, regardless of 
     * their internals.
     * 
     * @throws Exception
     *     if the catalog can't be configured properly.
     */
    @Test
    public void testComparison() throws Exception
    {
        Configuration one = new Configuration("one");
        Configuration two = new Configuration("two");

        Index indexA = allIndexes.indexes.get(0);
        Index indexB = allIndexes.indexes.get(1);

        one.add(indexA);
        one.add(indexB);

        two.add(indexB);
        two.add(indexA);

        Configuration oneB = new ConfigurationBitSet(one);
        Configuration twoB = new ConfigurationBitSet(two);

        assertThat(oneB, is(twoB));
    }

    /**
     * creates a global set of indexes.
     *
     * @throws Exception
     *     if the catalog can't be created accordingly
     */
    @BeforeClass
    public static void set_up() throws Exception
    {
        for (Index index : configureCatalog().<Schema>findByName("schema_0").indexes())
            allIndexes.add(index);
    }
}
