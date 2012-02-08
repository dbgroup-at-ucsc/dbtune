package edu.ucsc.dbtune.metadata;

import org.junit.Test;
import org.junit.BeforeClass;

import edu.ucsc.dbtune.metadata.Catalog;
import edu.ucsc.dbtune.metadata.Column;
import edu.ucsc.dbtune.metadata.DatabaseObject;
import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.metadata.Schema;
import edu.ucsc.dbtune.metadata.Table;

import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import static edu.ucsc.dbtune.DBTuneInstances.configureCatalog;
import static edu.ucsc.dbtune.metadata.DatabaseObject.NON_ID;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.notNullValue;

/**
 * Test for general metadata operations
 */
public class MetadataTest
{
    private static Catalog catalog;

    /**
     * Creates catalogs that are used by all the tests
     */
    @BeforeClass
    public static void setUp() throws Exception
    {
        catalog  = configureCatalog();
    }

    /**
     * Tests iterators
     */
    @Test
    public void testIterators() throws Exception
    {
        assertThat(catalog.getName(), is("catalog_0"));

        for (Schema schema : catalog)
        {
            assertThat(schema,is(notNullValue()));

            for (Table table : schema.tables())
            {
                assertThat(table,is(notNullValue()));

                for (Column column : table.columns())
                    assertThat(column,is(notNullValue()));
            }

            for (Index index : schema.indexes())
                assertThat(index,is(notNullValue()));
        }
    }

    /**
     * Tests that the schemas we created exists.
     */
    @Test
    public void testCatalogAndSchemaExists() throws Exception
    {
        assertThat(catalog.getName(), is("catalog_0"));

        for (Schema schema : catalog)
        {
            assertThat(schema.getName().equals("schema_0") || schema.getName().equals("schema_1"), is(true));
        }
    }

    /**
     * Tests that covers:
     *   - find(String)
     *   - convenience finds, eg: findSchema, findTable, etc
     *   - convenience gets, eg: getSchema, getTable, etc
     *   - the above implicitly checks getContainer()
     */
    @Test
    public void testContainment() throws Exception
    {
        assertThat(catalog.getContainer(), is(nullValue()));

        int j=0;
        for (Schema schema : catalog)
        {
            assertThat(catalog.find("schema_"+j++), is(notNullValue()));
            assertThat(schema.getCatalog(), is(catalog));

            for (int k = 0; k < 3; k++)
            {
                Table table = schema.findTable("table_"+k);

                assertThat(table, is(notNullValue()));
                assertThat(table.getSchema(), is(schema));

                for (int l = 0; l < 4; l++)
                {
                    Column column = table.findColumn("column_"+l);
                    assertThat(column, is(notNullValue()));
                    assertThat(column.getTable(), is(table));
                }
            }
        }
    }

    /**
     * Tests that checks that objects are ordered correctly in their containers. Also checks the 
     * {@link #at} method.
     */
    @Test
    public void testOrdinalPosition() throws Exception
    {
        int i=0, j=0;
        for (Schema schema : catalog)
        {
            assertThat(schema.getOrdinalPosition(), is(i+1));
            assertThat((Schema)catalog.at(i++), is(schema));
            // we don't care about Schemas but we're just leaving it for fun

            for (Table table : schema.tables())
            {
                // we don't care about Tables and Indexes
                // but we do care about positions of columns in tables

                j = 0;
                for (Column column : table.columns())
                {
                    assertThat(column.getOrdinalPosition(), is(j+1));
                    assertThat((Column)table.at(j++), is(column));
                }
            }
        }
    }

    /**
     * Tests that covers that fully qualified paths are handled correctly
     */
    @Test
    public void testFullyQualifiedAccess() throws Exception
    {
        assertThat(catalog.findByQualifiedName("schema_0"),is(notNullValue()));
        assertThat(catalog.findByQualifiedName("schema_0.table_0"),is(notNullValue()));
        assertThat(catalog.findByQualifiedName("schema_0.table_0.column_0"),is(notNullValue()));
        assertThat(catalog.findByQualifiedName("schema_0.table_0.column_0"),is(notNullValue()));
        assertThat(catalog.findByQualifiedName("schema_0.table_0_index_0"),is(notNullValue()));

        assertThat(catalog.findByQualifiedName("schema_32"),is(nullValue()));
        assertThat(catalog.findByQualifiedName("schema_0.table_32"),is(nullValue()));
        assertThat(catalog.findByQualifiedName("schema_0.table_0.column_32"),is(nullValue()));
        assertThat(catalog.findByQualifiedName("schema_0.table_0_index_32"),is(nullValue()));

        assertThat(catalog.findByQualifiedName("schema_0").getFullyQualifiedName(),
                   is("schema_0"));
        assertThat(catalog.findByQualifiedName("schema_0.table_0").getFullyQualifiedName(),
                   is("schema_0.table_0"));
        assertThat(catalog.findByQualifiedName("schema_0.table_0_index_0").getFullyQualifiedName(),
                   is("schema_0.table_0_index_0"));
        assertThat(catalog.findByQualifiedName("schema_0.table_0.column_0").getFullyQualifiedName(),
                   is("schema_0.table_0.column_0"));

    }

    /**
     * Tests containment validation. Also tests that {@link #findByName} works correctly
     */
    @Test
    public void testContainmentValidation() throws Exception
    { 
        Schema schema = catalog.<Schema>findByName("schema_0");
        Table  table  = catalog.<Table>findByName("schema_0.table_0");
        Index  index  = catalog.<Index>findByName("schema_0.table_0_index_0");
        Column column = catalog.<Column>findByName("schema_0.table_0.column_0");

        assertThat(catalog.isValid(catalog),is(false));
        assertThat(catalog.isValid(schema),is(true));
        assertThat(catalog.isValid(table),is(false));
        assertThat(catalog.isValid(index),is(false));
        assertThat(catalog.isValid(column),is(false));

        assertThat(schema.isValid(catalog),is(false));
        assertThat(schema.isValid(schema),is(false));
        assertThat(schema.isValid(table),is(true));
        assertThat(schema.isValid(index),is(true));
        assertThat(schema.isValid(column),is(false));

        assertThat(table.isValid(catalog),is(false));
        assertThat(table.isValid(schema),is(false));
        assertThat(table.isValid(table),is(false));
        assertThat(table.isValid(index),is(false));
        assertThat(table.isValid(column),is(true));

        assertThat(column.isValid(catalog),is(false));
        assertThat(column.isValid(schema),is(false));
        assertThat(column.isValid(table),is(false));
        assertThat(column.isValid(index),is(false));
        assertThat(column.isValid(column),is(false));
    }

    /**
     * Tests cardinality of database objects. Also checks {@link DatabaseObject#getAll}
     */
    @Test
    public void testCardinality() throws Exception
    {
        assertThat(catalog.size(),is(2));
        assertThat(catalog.find("schema_0").size(),is(3+45));
        assertThat(catalog.findByQualifiedName("schema_0.table_0").size(),is(4));
        assertThat(catalog.getAll().size(),is(
            1        + //   1 catalog
            2        + //   2 schemas
            (2*3)    + //   3 tables per schema
            (2*3*4)  + //   4 columns per table
            (2*3*15) + //  15 indexes per table
            (192)      // 192 columns in indexes
        ));
    }

    /**
     * Tests that objects that are created by using a fully qualified name are created correctly. 
     * Also tests {@link Catalog#createObject} and {@link}
     */
    @Test
    public void testCreationWithFullyQualifiedNames() throws Exception
    {
        Schema sch = catalog.<Schema>create("schema_3");
        Table tbl = new Table(sch,"table_0");
        Index idx = catalog.<Index>create("schema_3.table_0_index_0");
        Column col = catalog.<Column>create("schema_3.table_0.column_0");

        assertThat(sch, is(notNullValue()));
        assertThat(tbl, is(notNullValue()));
        assertThat(idx, is(notNullValue()));
        assertThat(col, is(notNullValue()));

        try {
            Table t = catalog.<Table>create("schema_3.table_1");
            assertThat(t.getFullyQualifiedName(), is("schema_3.table_1"));
            fail("catalog.<Table>create() should have thrown exception");
        } catch (ClassCastException ex) {
            assertThat(ex.getMessage(), is("edu.ucsc.dbtune.metadata.Index cannot be cast to edu.ucsc.dbtune.metadata.Table"));
        }
    }

    /**
     * Test unassignment of ID. Since no dbobject was assigned an ID explicitly in the construction 
     * of them, their ID should be {@link NON_ID}
     */
    @Test
    public void testID() throws Exception
    {
        for (DatabaseObject dbo : catalog.getAll()) {
            assertThat(dbo.getInternalID(),is(NON_ID));
        }
    }

    /**
     * todo:
     *   - find(int)
     *   - remove
     *   - equals
     *   - hashCode
     *   - toString
     */
}
