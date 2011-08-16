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

import java.util.List;
import java.util.ArrayList;
import java.util.Arrays;

import org.junit.Test;
import org.junit.BeforeClass;

import static org.junit.Assert.assertTrue;

import static edu.ucsc.dbtune.metadata.Index.CLUSTERED;
import static edu.ucsc.dbtune.metadata.Index.NON_UNIQUE;
import static edu.ucsc.dbtune.metadata.Index.PRIMARY;
import static edu.ucsc.dbtune.metadata.Index.SECONDARY;
import static edu.ucsc.dbtune.metadata.Index.UNCLUSTERED;
import static edu.ucsc.dbtune.metadata.Index.UNIQUE;

/**
 * Test for the Configuration class
 */
public class ConfigurationBitSetTest
{
    private static Catalog catalog;
    private static List<Index> allIndexes = new ArrayList<Index>();
    private static int id = 0;

    @BeforeClass
    public static void setUp() throws Exception {
        for(int i = 0; i < 1; i++) {
            catalog = new Catalog("catalog_" + i);
            for(int j = 0; j < 2; j++) {
                Schema schema = new Schema("schema_" + j);
                catalog.add(schema);
                for(int k = 0; k < 3; k++) {
                    Table table = new Table("table_" + k);
                    int l;
                    schema.add(table);
                    for(l = 0; l < 4; l++) {
                        Column column = new Column("column_" + l, l+1);
                        table.add(column);

                        Index index =
                            new Index(
                                "index_" + l, Arrays.asList(column), SECONDARY,UNCLUSTERED, NON_UNIQUE);
                        index.setId(id++);
                        table.add(index);
                        allIndexes.add(index);
                    }
                    Index index =
                        new Index(
                            "index_" + l, table.getColumns(), PRIMARY, CLUSTERED, UNIQUE);
                    table.add(index);
                    index.setId(id++);
                    allIndexes.add(index);
                }
            }
        }
    }

    @Test
    public void basicUsage()
    {
        IndexBitSet bitSet = new IndexBitSet();
        ConfigurationBitSet conf1 = new ConfigurationBitSet(allIndexes, bitSet);

        bitSet.set(allIndexes.get(0).getId());
        bitSet.set(allIndexes.get(allIndexes.size()-1).getId());

        assertTrue(conf1.contains(allIndexes.get(0)));
        assertTrue(conf1.contains(allIndexes.get(allIndexes.size()-1)));
    }
}
