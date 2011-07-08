/*
 * ****************************************************************************
 *   Copyright 2010 University of California Santa Cruz                       *
 *                                                                            *
 *   Licensed under the Apache License, Version 2.0 (the "License");          *
 *   you may not use this file except in compliance with the License.         *
 *   You may obtain a copy of the License at                                  *
 *                                                                            *
 *       http://www.apache.org/licenses/LICENSE-2.0                           *
 *                                                                            *
 *   Unless required by applicable law or agreed to in writing, software      *
 *   distributed under the License is distributed on an "AS IS" BASIS,        *
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. *
 *   See the License for the specific language governing permissions and      *
 *   limitations under the License.                                           *
 * ****************************************************************************
 */

package edu.ucsc.dbtune.core.metadata.extraction;

import edu.ucsc.dbtune.core.DatabaseConnection;
import edu.ucsc.dbtune.core.metadata.Catalog;
import edu.ucsc.dbtune.core.metadata.Schema;
import edu.ucsc.dbtune.core.metadata.Column;
import edu.ucsc.dbtune.core.metadata.Index;
import edu.ucsc.dbtune.core.metadata.Table;
import edu.ucsc.dbtune.util.SQLScriptExecuter;
import edu.ucsc.dbtune.spi.Environment;

import edu.ucsc.dbtune.util.Strings;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;

import static edu.ucsc.dbtune.core.JdbcConnectionManager.*;
import static org.junit.Assert.*;

/**
 * Test for the metadata extraction package.
 * <p>
 * The test assumes that a database has been configured in the <code>build.properties</code> file.  
 * Depending on the value for the <code>test.dbms.url</code> property, the test instantiates and 
 * calls the appropriate JDBCExtractorTest implementation.
 * <p>
 * Also, the file <code>movies.sql</code> in folder <code>test.dbms.script.folder</code> is expected 
 * to exist and contain the DBMS-dependant SQL statements that create and load the Movies database.  
 * <p>
 * Lastly, properties <code>test.dbms.username</code> and <code>test.dbms.password</code> are used 
 * to create new connections to the database, so they must also be provided.
 * <p>
 * The test fails entirely if the above two conditions aren't met.
 *
 * @author Ivo Jimenez
 */
public class MetaDataExtractorTestFunctional
{
    private static DatabaseConnection   connection;
    private static GenericJDBCExtractor extractor;
    private static Catalog              catalog;
    private static Schema               schema;
    private static Environment          environment;

    private static final String RATINGS     = "ratings";
    private static final String QUEUE       = "queue";
    private static final String CASTS       = "casts";
    private static final String ACTORS      = "actors";
    private static final String GENRES      = "genres";
    private static final String MOVIES      = "movies";
    private static final String CREDITCARDS = "creditcards";
    private static final String USERS       = "users";

    /**
     * Executes the SQL script that should contain the 'movies' database, then extracts the metadata 
     * for this database using the appropriate <code>MetaDataExtractor</code> implementor.
     */
    @BeforeClass
    public static void setUp() throws Exception
    {
        String ddlfilename;

        environment = Environment.getInstance();
        connection  = makeDatabaseConnectionManager(environment.getAll()).connect();
        ddlfilename = environment.getScriptAtWorkloadsFolder("movies/create.sql");

        SQLScriptExecuter.execute(connection.getJdbcConnection(), ddlfilename);

        extractor = Strings.contains(environment.getJDBCDriver(), "postgresql")
                ? new PGExtractor()
                : new GenericJDBCExtractor();

        catalog = extractor.extract(connection);
    }

    @AfterClass
    public static void tearDown() throws Exception
    {
        if(connection != null) connection.close();
    }

    /**
     * Tests that the schema we created exists.
     */
    @Test
    public void testSchemaExists() throws Exception
    {
        Schema moviesSchema = null;

        for (Schema schema : catalog.getSchemas())
        {
            if(schema.getName().equals("movies")) {
                moviesSchema = schema;
            }
        }

        assertTrue(moviesSchema != null);

        schema = moviesSchema;
    }

    /**
     * Tests the size of database object containers
     */
    @Test
    public void testContainment() throws Exception
    {
        List<Column> columns;
        List<Index>  indexes;

        assertTrue("catalog should have at leas 'movies' schema", catalog.getSchemas().size() >= 1);
        assertEquals("'movies' schema has 8 tables", 8, schema.getTables().size());

        for (Table table : schema.getTables())
        {
            columns = table.getColumns();
            indexes = table.getIndexes();

            if (Strings.same(table.getName(), USERS))
            {
                assertEquals("columns in 'users'", 5, columns.size());
                assertEquals("indexes in 'users'", 2, indexes.size());
            }
            else if (Strings.same(table.getName(), CREDITCARDS))
            {
                assertEquals("columns in 'creditcards'", 4, columns.size());
                assertEquals("indexes in 'creditcards'", 2, indexes.size());
            }
            else if (Strings.same(table.getName(), MOVIES))
            {
                assertEquals("columns in 'movies'", 5, columns.size());
                assertEquals("indexes in 'movies'", 2, indexes.size());
            }
            else if (Strings.same(table.getName(), GENRES))
            {
                assertEquals("columns in 'genres'", 2, columns.size());
                assertEquals("indexes in 'genres'", 1, indexes.size());
            }
            else if (Strings.same(table.getName(), ACTORS))
            {
                assertEquals("columns in 'actors'", 4, columns.size());
                assertEquals("indexes in 'actors'", 2, indexes.size());
            }
            else if (Strings.same(table.getName(), CASTS))
            {
                assertEquals("columns in 'casts'", 2, columns.size());
                assertEquals("indexes in 'casts'", 1, indexes.size());
            }
            else if (Strings.same(table.getName(), QUEUE))
            {
                assertEquals("columns in 'queue'", 4, columns.size());
                assertEquals("indexes in 'queue'", 2, indexes.size());
            }
            else if (Strings.same(table.getName(), RATINGS))
            {
                assertEquals("columns in 'ratings'", 4, columns.size());
                assertEquals("indexes in 'ratings'", 0, indexes.size());
            }
            else
            {
                fail("Unexpected table " + table.getName());
            }
        }
    }

    /**
     * Checks that the ordering of columns on each table and index is correct. The test makes sure 
     * that the elements on the list that is returned by a container (eg.  
     * <code>columns</code>), when iterated using the <code>List.iterator()</code>  
     * method, are in the correct order.
     */
    @Test
    public void testColumnOrdering() throws Exception
    {
        List<Column> columns;

        for (Table table : schema.getTables())
        {
            columns = table.getColumns();

            if (Strings.same(table.getName(), MOVIES))
            {
                for (int i = 0; i < columns.size(); i++)
                {
                    if (i == 0)
                    {
                        assertEquals("movieid", columns.get(i).getName());
                    }
                    else if (i == 1)
                    {
                        assertEquals("title", columns.get(i).getName());
                    }
                    else if (i == 2)
                    {
                        assertEquals("yearofr", columns.get(i).getName());
                    }
                    else if (i == 3)
                    {
                        assertEquals("summary", columns.get(i).getName());
                    }
                    else if (i == 4)
                    {
                        assertEquals("url", columns.get(i).getName());
                    }
                }
            }
            else if (Strings.same(table.getName(), ACTORS))
            {
                for (int i = 0; i < columns.size(); i++)
                {
                    if (i == 0)
                    {
                        assertEquals("aid", columns.get(i).getName());
                    }
                    else if (i == 1)
                    {
                        assertEquals("afirstname", columns.get(i).getName());
                    }
                    else if (i == 2)
                    {
                        assertEquals("alastname", columns.get(i).getName());
                    }
                    else if (i == 3)
                    {
                        assertEquals("dateofb", columns.get(i).getName());
                    }
                }
            }
        }
    }

    /**
     * Checks that all the indexes that are defined on the <code>movies</code> database are 
     * extracted correctly. This tests looks only at the name of the indexes.
     */
    @Test
    public void testIndexes() throws Exception
    {
        for (Table table : schema.getTables())
        {
            if (Strings.same(table.getName(), USERS))
            {
                assertTrue(table.findIndex("users_pkey") != null);
                assertTrue(table.findIndex("users_userid_key") != null);
            }
            else if (Strings.same(table.getName(), CREDITCARDS))
            {
                assertTrue(table.findIndex("creditcards_pkey") != null);
                assertTrue(table.findIndex("creditcards_creditnum_key") != null);
            }
            else if (Strings.same(table.getName(), MOVIES))
            {
                assertTrue(table.findIndex("movies_pkey") != null);
                assertTrue(table.findIndex("movies_movieid_key") != null);
            }
            else if (Strings.same(table.getName(), GENRES))
            {
                assertTrue(table.findIndex("genres_pkey") != null);
            }
            else if (Strings.same(table.getName(), ACTORS))
            {
                assertTrue(table.findIndex("actors_pkey") != null);
                assertTrue(table.findIndex("actors_afirstname_key") != null);
            }
            else if (Strings.same(table.getName(), CASTS))
            {
                assertTrue(table.findIndex("casts_pkey") != null);
            }
            else if (Strings.same(table.getName(), QUEUE))
            {
                assertTrue(table.findIndex("queue_pkey") != null);
                assertTrue(table.findIndex("queue_times_key") != null);
            }
            else if (Strings.same(table.getName(), RATINGS))
            {
                // none
            }
            else
            {
                fail("Unexpected table " + table.getName());
            }
        }
    }

    /**
     * Tests cardinality of database objects. Checks that columns and tables have the expected 
     * cardinality. For columns, this corresponds to the count of unique entries; for tables, the 
     * number of rows contained in it.
     */
    @Test
    public void testCardinality() throws Exception
    {
        for (Table table : schema.getTables())
        {
            if (Strings.same(table.getName(), USERS))
            {
                assertEquals("rows in 'users'", 6, table.getCardinality());
                assertEquals(6,table.findColumn("email").getCardinality());
                assertEquals(5,table.findColumn("ulastname").getCardinality());
            }
            else if (Strings.same(table.getName(), CREDITCARDS))
            {
                assertEquals("rows in 'creditcards'", 6, table.getCardinality());
                assertEquals(6,table.findColumn("creditnum").getCardinality());
                assertEquals(3,table.findColumn("credittype").getCardinality());
            }
            else if (Strings.same(table.getName(), MOVIES))
            {
                assertEquals("rows in 'movies'", 3, table.getCardinality());
                assertEquals(3,table.findColumn("movieid").getCardinality());
                assertEquals(1,table.findColumn("url").getCardinality());
            }
            else if (Strings.same(table.getName(), GENRES))
            {
                assertEquals("rows in 'genres'", 5, table.getCardinality());
                assertEquals(2,table.findColumn("mgenre").getCardinality());
            }
            else if (Strings.same(table.getName(), ACTORS))
            {
                assertEquals("rows in 'actors'", 8, table.getCardinality());
                assertEquals(8,table.findColumn("aid").getCardinality());
                assertEquals(8,table.findColumn("dateofb").getCardinality());
            }
            else if (Strings.same(table.getName(), CASTS))
            {
                assertEquals("rows in 'casts'", 10, table.getCardinality());
            }
            else if (Strings.same(table.getName(), QUEUE))
            {
                assertEquals("rows in 'queue'", 5, table.getCardinality());
                assertEquals(2,table.findColumn("position").getCardinality());
                assertEquals(5,table.findColumn("times").getCardinality());
            }
            else if (Strings.same(table.getName(), RATINGS))
            {
                assertEquals("rows in 'ratings'", 1000, table.getCardinality());
                assertEquals(4,table.findColumn("rate").getCardinality());
                assertEquals(8,table.findColumn("review").getCardinality());
            }
            else
            {
                fail("Unexpected table " + table.getName());
            }
        }
    }

    /**
     * Tests size of database objects. This test ensures that the size of each database object (in 
     * bytes) is correct. For columns the size is determined by the data type. For tables, the size 
     * in bytes is just the sum of all the columns contained in it (which is sometimes referred to 
     * as row size).
     */
    @Test
    public void testSize() throws Exception
    {
    }
}
