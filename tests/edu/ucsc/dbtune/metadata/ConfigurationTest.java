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
