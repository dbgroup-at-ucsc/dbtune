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
package edu.ucsc.dbtune.core.metadata;

import java.util.List;
import java.util.ArrayList;
import java.util.Arrays;

import org.junit.Test;
import org.junit.BeforeClass;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import static edu.ucsc.dbtune.core.metadata.Index.PRIMARY;
import static edu.ucsc.dbtune.core.metadata.Index.SECONDARY;
import static edu.ucsc.dbtune.core.metadata.Index.CLUSTERED;
import static edu.ucsc.dbtune.core.metadata.Index.UNCLUSTERED;
import static edu.ucsc.dbtune.core.metadata.Index.UNIQUE;
import static edu.ucsc.dbtune.core.metadata.Index.NON_UNIQUE;

/**
 * Test for general metadata operations
 */
public class MetadataTest
{
    private static Catalog catalog;
    private static List<DatabaseObject> allObjects;

    /**
     * Creates catalogs that are used by all the tests
     */
    @BeforeClass
    public static void setUp() throws Exception {
        allObjects = new ArrayList<DatabaseObject>();
        for(int i = 0; i < 1; i++) {
            catalog = new Catalog("catalog_" + i);
            allObjects.add(catalog);
            for(int j = 0; j < 2; j++) {
                Schema schema = new Schema("schema_" + j);
                catalog.add(schema);
                allObjects.add(schema);
                for(int k = 0; k < 3; k++) {
                    Table table = new Table("table_" + k);
                    int l;
                    schema.add(table);
                    allObjects.add(table);
                    for(l = 0; l < 4; l++) {
                        Column column = new Column("column_" + l, l+1);
                        table.add(column);
                        allObjects.add(column);

                        Index index =
                            new Index(
                                "index_" + l, Arrays.asList(column), PRIMARY,CLUSTERED, UNIQUE);
                        table.add(index);
                        allObjects.add(index);
                    }
                    Index index =
                        new Index(
                            "index_" + l, table.getColumns(), SECONDARY, UNCLUSTERED, NON_UNIQUE);
                    table.add(index);
                    allObjects.add(index);
                }
            }
        }
    }

    /**
     * Tests that the schemas we created exists.
     */
    @Test
    public void testCatalogAndSchemaExists() throws Exception
    {
        assertTrue(catalog.getName().equals("catalog_0"));

        for (Schema schema : catalog.getSchemas())
        {
            assertTrue(schema.getName().equals("schema_0") || schema.getName().equals("schema_1"));
        }
    }

    /**
     * Tests the size of database object containers
     */
    @Test
    public void testContainment() throws Exception
    {
        // also tests the DatabaseObject.findByName() method

        for(int j = 0; j < 2; j++) {
            Schema schema = catalog.getSchemas().get(j);
            assertEquals(schema, catalog.findSchema("schema_"+j));
            for(int k = 0; k < 3; k++) {
                Table table = schema.getTables().get(k);
                int l;
                assertEquals(table, schema.findTable("table_"+k));
                for(l = 0; l < 4; l++) {
                    Column column = table.getColumns().get(l);
                    Index index = table.getIndexes().get(l);
                    assertEquals(column, table.findColumn("column_"+l));
                    assertEquals(index, table.findIndex("index_"+l));
                }
                Index index = table.getIndexes().get(l);
                assertEquals(index, table.findIndex("index_"+l));
            }
        }
    }

    /**
     * Tests cardinality of database objects.
     */
    @Test
    public void testCardinality() throws Exception
    {
        assertEquals(2, catalog.getSchemas().size());
        assertEquals(3, catalog.getSchemas().get(0).getTables().size());
        assertEquals(4, catalog.getSchemas().get(0).getTables().get(0).getColumns().size());
    }

    /**
     * Test unassignment of ID. Since no dbobject was assigned an ID explicitly in the construction 
     * of them, their ID should be -1
     */
    @Test
    public void testID() throws Exception
    {
        for(DatabaseObject dbo : allObjects) {
            assertEquals(-1,dbo.getId());
        }
    }
}
