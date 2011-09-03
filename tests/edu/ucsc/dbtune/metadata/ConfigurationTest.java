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
public class ConfigurationTest
{
    private static Catalog catalog;
    private static List<Index> allIndexes = new ArrayList<Index>();

    @BeforeClass
    public static void setUp() throws Exception {
        for(int i = 0; i < 1; i++) {
            catalog = new Catalog("catalog_" + i);
            for(int j = 0; j < 2; j++) {
                Schema schema = new Schema(catalog,"schema_" + j);
                for(int k = 0; k < 3; k++) {
                    Table table = new Table(schema,"table_" + k);
                    int l;
                    for(l = 0; l < 4; l++) {
                        Column column = new Column(table,"column_" + l, l+1);

                        Index index =
                            new Index(
                                "index_" + l, Arrays.asList(column), SECONDARY,UNCLUSTERED, NON_UNIQUE);
                        allIndexes.add(index);
                    }
                    Index index =
                        new Index(
                            "index_" + l, table.getColumns(), PRIMARY, CLUSTERED, UNIQUE);
                    allIndexes.add(index);
                }
            }
        }
    }

    @Test
    public void testPopulatingConfiguration() throws Exception {
        Configuration conf1 = new Configuration(allIndexes);

        for(Index idx : allIndexes) {
            assertTrue(conf1.contains(idx));
        }
    }
}
