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
import edu.ucsc.dbtune.core.metadata.Column;
import edu.ucsc.dbtune.core.metadata.Index;
import edu.ucsc.dbtune.core.metadata.Table;
import edu.ucsc.dbtune.util.SQLScriptExecuter;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;
import java.util.Properties;

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
 * @author ivo@cs.ucsc.edu (Ivo Jimenez)
 */
public class MetaDataExtractorTest
{
    private static DatabaseConnection   con;
    private static GenericJDBCExtractor extractor;
    private static Catalog              catalog;

    /**
     * Executes the SQL script that should contain the 'movies' database, then extracts the metadata 
     * for this database using the appropriate <code>MetaDataExtractor</code> implementor.
     */
    @BeforeClass
    public static void setUp() throws Exception
    {
        Properties props;
        String     file;

        props = new Properties();

        props.setProperty(URL,      System.getProperty("test.dbms.url"));
        props.setProperty(USERNAME, System.getProperty("test.dbms.username"));
        props.setProperty(PASSWORD, System.getProperty("test.dbms.password"));
        props.setProperty(DATABASE, System.getProperty("test.dbms.database"));
        props.setProperty(DRIVER,   System.getProperty("test.dbms.driver"));

        con  = makeDatabaseConnectionManager(props).connect();
        file = System.getProperty("test.dbms.script.dir") + "/movies.sql";

        SQLScriptExecuter.execute(con.getJdbcConnection(), file);

        if (props.getProperty(DRIVER).contains("postgresql"))
        {
            extractor = new PGExtractor();
        }
        else
        {
            extractor = new GenericJDBCExtractor();
        }

        catalog = extractor.extract(con);
    }

    @AfterClass
    public static void tearDown() throws Exception
    {
        con.close();
    }

    /**
     * Tests the size of database object containers
     */
    @Test
    public void testContainment() throws Exception
    {
        List<Column> columns;
        List<Index> indexes;

        assertEquals("'movies' is the only schema",  1, catalog.getSchemas().size());
        assertEquals("'movies' schema has 8 tables", 8, catalog.getSchemas().get(0).getTables().size());

        for (Table table : catalog.getSchemas().get(0).getTables())
        {
            columns = table.getColumns();
            indexes = table.getIndexes();

            if (table.getName().equals("users"))
            {
                assertEquals("columns in 'users'", 5, columns.size());
                assertEquals("indexes in 'users'", 2, indexes.size());
            }
            else if (table.getName().equals("creditcards"))
            {
                assertEquals("columns in 'creditcards'", 4, columns.size());
                assertEquals("indexes in 'creditcards'", 2, indexes.size());
            }
            else if (table.getName().equals("movies"))
            {
                assertEquals("columns in 'movies'", 5, columns.size());
                assertEquals("indexes in 'movies'", 2, indexes.size());
            }
            else if (table.getName().equals("genres"))
            {
                assertEquals("columns in 'genres'", 2, columns.size());
                assertEquals("indexes in 'genres'", 1, indexes.size());
            }
            else if (table.getName().equals("actors"))
            {
                assertEquals("columns in 'actors'", 4, columns.size());
                assertEquals("indexes in 'actors'", 2, indexes.size());
            }
            else if (table.getName().equals("casts"))
            {
                assertEquals("columns in 'casts'", 2, columns.size());
                assertEquals("indexes in 'casts'", 1, indexes.size());
            }
            else if (table.getName().equals("queue"))
            {
                assertEquals("columns in 'queue'", 4, columns.size());
                assertEquals("indexes in 'queue'", 2, indexes.size());
            }
            else if (table.getName().equals("ratings"))
            {
                assertEquals("columns in 'ratings'", 4, columns.size());
                assertEquals("indexes in 'ratings'", 1, indexes.size());
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

        for (Table table : catalog.getSchemas().get(0).getTables())
        {
            columns = table.getColumns();

            if (table.getName().equals("movies"))
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
            else if (table.getName().equals("actors"))
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
        for (Table table : catalog.getSchemas().get(0).getTables())
        {
            if (table.getName().equals("users"))
            {
                assertTrue(table.findIndex("users_pkey") != null);
                assertTrue(table.findIndex("users_userid_key") != null);
            }
            else if (table.getName().equals("creditcards"))
            {
                assertTrue(table.findIndex("creditcards_pkey") != null);
                assertTrue(table.findIndex("creditcards_creditnum_key") != null);
            }
            else if (table.getName().equals("movies"))
            {
                assertTrue(table.findIndex("movies_pkey") != null);
                assertTrue(table.findIndex("movies_movieid_key") != null);
            }
            else if (table.getName().equals("genres"))
            {
                assertTrue(table.findIndex("genres_pkey") != null);
            }
            else if (table.getName().equals("actors"))
            {
                assertTrue(table.findIndex("actors_pkey") != null);
                assertTrue(table.findIndex("actors_afirstname_key") != null);
            }
            else if (table.getName().equals("casts"))
            {
                assertTrue(table.findIndex("casts_pkey") != null);
            }
            else if (table.getName().equals("queue"))
            {
                assertTrue(table.findIndex("queue_pkey") != null);
                assertTrue(table.findIndex("queue_times_key") != null);
            }
            else if (table.getName().equals("ratings"))
            {
                assertTrue(table.findIndex("ratings_pkey") != null);
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
        for (Table table : catalog.getSchemas().get(0).getTables())
        {
            if (table.getName().equals("users"))
            {
                assertEquals("rows in 'users'", 6, table.getCardinality());
                assertEquals(6,table.findColumn("email").getCardinality());
                assertEquals(5,table.findColumn("ulastname").getCardinality());
            }
            else if (table.getName().equals("creditcards"))
            {
                assertEquals("rows in 'creditcards'", 6, table.getCardinality());
                assertEquals(6,table.findColumn("creditnum").getCardinality());
                assertEquals(3,table.findColumn("credittype").getCardinality());
            }
            else if (table.getName().equals("movies"))
            {
                assertEquals("rows in 'movies'", 3, table.getCardinality());
                assertEquals(3,table.findColumn("movieid").getCardinality());
                assertEquals(1,table.findColumn("url").getCardinality());
            }
            else if (table.getName().equals("genres"))
            {
                assertEquals("rows in 'genres'", 5, table.getCardinality());
                assertEquals(2,table.findColumn("mgenre").getCardinality());
            }
            else if (table.getName().equals("actors"))
            {
                assertEquals("rows in 'actors'", 8, table.getCardinality());
                assertEquals(8,table.findColumn("aid").getCardinality());
                assertEquals(8,table.findColumn("dateofb").getCardinality());
            }
            else if (table.getName().equals("casts"))
            {
                assertEquals("rows in 'casts'", 10, table.getCardinality());
            }
            else if (table.getName().equals("queue"))
            {
                assertEquals("rows in 'queue'", 5, table.getCardinality());
                assertEquals(2,table.findColumn("position").getCardinality());
                assertEquals(5,table.findColumn("times").getCardinality());
            }
            else if (table.getName().equals("ratings"))
            {
                assertEquals("rows in 'ratings'", 7, table.getCardinality());
                assertEquals(4,table.findColumn("rate").getCardinality());
                assertEquals(7,table.findColumn("review").getCardinality());
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
