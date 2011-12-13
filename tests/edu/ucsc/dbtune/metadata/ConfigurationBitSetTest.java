package edu.ucsc.dbtune.metadata;

import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.metadata.Schema;
import edu.ucsc.dbtune.util.IndexBitSet;

import org.junit.Test;
import org.junit.BeforeClass;

import static org.junit.Assert.assertThat;

import static edu.ucsc.dbtune.DBTuneInstances.configureCatalog;

import static org.hamcrest.Matchers.is;

/**
 * Test for the Configuration class
 */
public class ConfigurationBitSetTest
{
    private static Configuration allIndexes = new Configuration("all");

    @BeforeClass
    public static void setUp() throws Exception
    {
        for (Index index : configureCatalog().<Schema>findByName("schema_0").indexes())
            allIndexes.add(index);
    }

    @Test
    public void basicUsage()
    {
        IndexBitSet bitSet = new IndexBitSet();

        bitSet.set(0);
        bitSet.set(allIndexes.size()-2);

        ConfigurationBitSet conf1 = new ConfigurationBitSet(allIndexes, bitSet);

        assertThat(conf1.contains(allIndexes.getIndexAt(0)), is(true));
        assertThat(conf1.contains(allIndexes.getIndexAt(allIndexes.size()-2)), is(true));
    }
}
