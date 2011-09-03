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

import edu.ucsc.dbtune.metadata.Catalog;
import edu.ucsc.dbtune.metadata.Column;
import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.metadata.Schema;
import edu.ucsc.dbtune.metadata.Table;
import edu.ucsc.dbtune.util.IndexBitSet;

import java.util.Arrays;

import org.junit.Test;
import org.junit.BeforeClass;

import static org.junit.Assert.assertThat;

import static edu.ucsc.dbtune.metadata.Index.CLUSTERED;
import static edu.ucsc.dbtune.metadata.Index.NON_UNIQUE;
import static edu.ucsc.dbtune.metadata.Index.PRIMARY;
import static edu.ucsc.dbtune.metadata.Index.SECONDARY;
import static edu.ucsc.dbtune.metadata.Index.UNCLUSTERED;
import static edu.ucsc.dbtune.metadata.Index.UNIQUE;

import static org.hamcrest.Matchers.is;

/**
 * Test for the Configuration class
 */
public class ConfigurationBitSetTest
{
    private static Catalog catalog;
    private static Configuration allIndexes = new Configuration("all");

    @BeforeClass
    public static void setUp() throws Exception {
        for(int i = 0; i < 1; i++) {
            catalog = new Catalog("catalog_" + i);
            for(int j = 0; j < 2; j++) {
                Schema schema = new Schema(catalog,"schema_" + j);
                for(int k = 0; k < 3; k++) {
                    Table table = new Table(schema,"table_" + k);
                    int count = 0;
                    for(int l = 0; l < 4; l++) {
                        Column column = new Column(table,"column_" + count++, l+1);

                        Index index =
                            new Index(
                                "index_" + count++, Arrays.asList(column), SECONDARY,UNCLUSTERED, NON_UNIQUE);
                        allIndexes.add(index);
                    }
                    Index index =
                        new Index(
                            "index_" + count++, table.getColumns(), PRIMARY, CLUSTERED, UNIQUE);
                    allIndexes.add(index);
                }
            }
        }
    }

    @Test
    public void basicUsage()
    {
        IndexBitSet bitSet = new IndexBitSet();

        bitSet.set(0);
        bitSet.set(allIndexes.size()-2);

        ConfigurationBitSet conf1 = new ConfigurationBitSet(allIndexes, bitSet);

        assertThat(conf1.contains(allIndexes.getIndexes().get(0)), is(true));
        assertThat(conf1.contains(allIndexes.getIndexes().get(allIndexes.size()-2)), is(true));
    }
}
