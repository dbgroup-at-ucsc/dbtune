package edu.ucsc.dbtune.metadata;

import edu.ucsc.dbtune.metadata.Index;

import java.util.List;
import java.util.ArrayList;

import org.junit.Test;
import org.junit.BeforeClass;

import static edu.ucsc.dbtune.DBTuneInstances.configureCatalog;

import static org.junit.Assert.assertTrue;

/**
 * Test for the Configuration class
 */
public class ConfigurationTest
{
    private static List<Index> allIndexes = new ArrayList<Index>();

    @BeforeClass
    public static void setUp() throws Exception {
        for(Index index : configureCatalog().<Schema>findByName("schema_0").indexes())
            allIndexes.add(index);
    }

    @Test
    public void testPopulatingConfiguration() throws Exception {
        Configuration conf1 = new Configuration(allIndexes);

        for(Index idx : allIndexes) {
            assertTrue(conf1.contains(idx));
        }
    }
}
