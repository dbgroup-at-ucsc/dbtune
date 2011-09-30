/* *************************************************************************** *
 *   Copyright 2010 University of California Santa Cruz                        *
 *                                                                             *
 *   Licensed under the Apache License, Version 2.0 (the "License");           *
 *   you may not use this file except in compliance with the License.          *
 *   You may obtain a copy of the License at                                   *
 *                                                                             *
 *       http://www.apache.org/licenses/LICENSE-2.0                            *
 *                                                                             *
 *   Unless required by applicable law or agreed to in writing, software       *
 *   distributed under the License is distributed on an "AS IS" BASIS,         *
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  *
 *   See the License for the specific language governing permissions and       *
 *   limitations under the License.                                            *
 * *************************************************************************** */
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
    public static void setUp() throws Exception {
        for(Index index : configureCatalog().<Schema>findByName("schema_0").indexes())
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
