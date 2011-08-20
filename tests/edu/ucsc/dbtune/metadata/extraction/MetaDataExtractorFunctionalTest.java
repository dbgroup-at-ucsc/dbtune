/* ************************************************************************** *
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
 * ************************************************************************** */
package edu.ucsc.dbtune.metadata.extraction;

import edu.ucsc.dbtune.metadata.Catalog;
import edu.ucsc.dbtune.metadata.Column;
import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.metadata.Schema;
import edu.ucsc.dbtune.metadata.Table;
import edu.ucsc.dbtune.spi.Environment;
import edu.ucsc.dbtune.util.Strings;

import java.sql.Connection;
import java.util.List;

import org.junit.BeforeClass;
import org.junit.Test;

import static edu.ucsc.dbtune.DatabaseSystem.newConnection;
import static edu.ucsc.dbtune.DatabaseSystem.newExtractor;
import static edu.ucsc.dbtune.util.SQLScriptExecuter.execute;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.hamcrest.CoreMatchers.is;

/**
 * Test for the metadata extraction functionality. This test assumes that the system on the backend 
 * passes the {@link GenericJDBCExtractorTestFunctional} test.
 *
 * @author Ivo Jimenez
 * @see GenericJDBCExtractorTestFunctional
 */
public class MetaDataExtractorFunctionalTest
{
    private static Environment env;
    private static Connection  con;
    private static Catalog     cat;
    private static Schema      sch;

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
     * for this database using the appropriate {@link MetadataExtractor} implementor.
     */
    @BeforeClass
    public static void setUp() throws Exception
    {
        String ddl;

        env = Environment.getInstance();
        ddl = env.getScriptAtWorkloadsFolder("movies/create.sql");
        con = newConnection(env);

        //execute(con, ddl);

        cat = newExtractor(env).extract(con);

        con.close();
    }

    /**
     * Tests that the catalog we created exists.
     */
    @Test
    public void testCatalogExists() throws Exception
    {
        assertThat(cat != null, is(true));
    }

    /**
     * Tests that the schema we created exists.
     */
    @Test
    public void testSchemaExists() throws Exception
    {
        assertThat(cat.getSchemas().size() > 1, is(true));

        sch = cat.findSchema("movies");

        assertThat(sch != null, is(true));
    }

    /**
     * Tests tables and columns exist
     */
    @Test
    public void testTablesAndColumnsExist() throws Exception
    {
        List<Column> columns;
        int          expected;

        assertThat(cat.getSchemas().size() >= 1, is(true));
        assertThat(sch.getTables().size(), is(8));

        for (Table tbl : sch.getTables())
        {
            columns  = tbl.getColumns();
            expected = -1;

            if (Strings.same(tbl.getName(), USERS))
            {
                expected = 5;
            }
            else if (Strings.same(tbl.getName(), CREDITCARDS))
            {
                expected = 4;
            }
            else if (Strings.same(tbl.getName(), MOVIES))
            {
                expected = 5;
            }
            else if (Strings.same(tbl.getName(), GENRES))
            {
                expected = 2;
            }
            else if (Strings.same(tbl.getName(), ACTORS))
            {
                expected = 4;
            }
            else if (Strings.same(tbl.getName(), CASTS))
            {
                expected = 2;
            }
            else if (Strings.same(tbl.getName(), QUEUE))
            {
                expected = 4;
            }
            else if (Strings.same(tbl.getName(), RATINGS))
            {
                expected = 4;
            }
            else
            {
                fail("Unexpected table " + tbl.getName());
            }

            assertThat(columns.size(), is(expected));
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
        String       expected;

        for (Table tbl : sch.getTables())
        {
            columns  = tbl.getColumns();
            expected = "";

            if (Strings.same(tbl.getName(), MOVIES))
            {
                for (int i = 0; i < columns.size(); i++)
                {
                    if (i == 0)
                    {
                        expected = "movieid";
                    }
                    else if (i == 1)
                    {
                        expected = "title";
                    }
                    else if (i == 2)
                    {
                        expected = "yearofr";
                    }
                    else if (i == 3)
                    {
                        expected = "summary";
                    }
                    else if (i == 4)
                    {
                        expected = "url";
                    }
                    assertThat(columns.get(i).getName(), is(expected));
                }
            }
            else if (Strings.same(tbl.getName(), ACTORS))
            {
                for (int i = 0; i < columns.size(); i++)
                {
                    if (i == 0)
                    {
                        expected = "aid";
                    }
                    else if (i == 1)
                    {
                        expected = "afirstname";
                    }
                    else if (i == 2)
                    {
                        expected = "alastname";
                    }
                    else if (i == 3)
                    {
                        expected = "dateofb";
                    }
                    assertThat(columns.get(i).getName(), is(expected));
                }
            }
        }
    }
    /**
     * Tests indexes exist
     */
    @Test
    public void testIndexesExist() throws Exception
    {
        List<Index>  indexes;
        int          expectedSize = -1;

        sch = cat.findSchema("movies");

        assertThat(sch != null, is(true));
        assertThat(cat.getSchemas().size() >= 1, is(true));
        assertThat(sch.getTables().size(), is(8));

        for (Table tbl : sch.getTables())
        {
            indexes = tbl.getIndexes();

            if (Strings.same(tbl.getName(), USERS))
            {
                expectedSize = 2;
                assertThat(tbl.findIndex("users_pkey") != null, is(true));
                assertThat(tbl.findIndex("users_userid_email") != null, is(true));
            }
            else if (Strings.same(tbl.getName(), CREDITCARDS))
            {
                expectedSize = 2;
                assertThat(tbl.findIndex("creditcards_pkey") != null, is(true));
                assertThat(tbl.findIndex("creditcards_creditnum_userid_credittype") != null, is(true));
            }
            else if (Strings.same(tbl.getName(), MOVIES))
            {
                expectedSize = 2;
                assertThat(tbl.findIndex("movies_pkey") != null, is(true));
                assertThat(tbl.findIndex("movies_moiveid_title_yearofr") != null, is(true));
            }
            else if (Strings.same(tbl.getName(), GENRES))
            {
                expectedSize = 1;
                assertThat(tbl.findIndex("genres_pkey") != null, is(true));
            }
            else if (Strings.same(tbl.getName(), ACTORS))
            {
                expectedSize = 2;
                assertThat(tbl.findIndex("actors_pkey") != null, is(true));
                assertThat(tbl.findIndex("actors_afirstname_alastname_dateofb") != null, is(true));
            }
            else if (Strings.same(tbl.getName(), CASTS))
            {
                expectedSize = 1;
                assertThat(tbl.findIndex("casts_pkey") != null, is(true));
            }
            else if (Strings.same(tbl.getName(), QUEUE))
            {
                expectedSize = 2;
                assertThat(tbl.findIndex("queue_pkey") != null, is(true));
                assertThat(tbl.findIndex("queue_times") != null, is(true));
            }
            else if (Strings.same(tbl.getName(), RATINGS))
            {
                expectedSize = 0;
            }
            else
            {
                fail("Unexpected table " + tbl.getName());
            }

            assertThat(indexes.size() >= expectedSize, is(true));
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
        for (Table tbl : sch.getTables())
        {
            if (Strings.same(tbl.getName(), USERS))
            {
                assertEquals(6,tbl.getCardinality());
                assertEquals(6,tbl.findColumn("email").getCardinality());
                assertEquals(5,tbl.findColumn("ulastname").getCardinality());
            }
            else if (Strings.same(tbl.getName(), CREDITCARDS))
            {
                assertEquals(6,tbl.getCardinality());
                assertEquals(6,tbl.findColumn("creditnum").getCardinality());
                assertEquals(3,tbl.findColumn("credittype").getCardinality());
            }
            else if (Strings.same(tbl.getName(), MOVIES))
            {
                assertEquals(3,tbl.getCardinality());
                assertEquals(3,tbl.findColumn("movieid").getCardinality());
                assertEquals(1,tbl.findColumn("url").getCardinality());
            }
            else if (Strings.same(tbl.getName(), GENRES))
            {
                assertEquals(5,tbl.getCardinality());
                assertEquals(2,tbl.findColumn("mgenre").getCardinality());
            }
            else if (Strings.same(tbl.getName(), ACTORS))
            {
                assertEquals(8,tbl.getCardinality());
                assertEquals(8,tbl.findColumn("aid").getCardinality());
                assertEquals(8,tbl.findColumn("dateofb").getCardinality());
            }
            else if (Strings.same(tbl.getName(), CASTS))
            {
                assertEquals(10,tbl.getCardinality());
            }
            else if (Strings.same(tbl.getName(), QUEUE))
            {
                assertEquals(5,tbl.getCardinality());
                assertEquals(2,tbl.findColumn("position").getCardinality());
                assertEquals(5,tbl.findColumn("times").getCardinality());
            }
            else if (Strings.same(tbl.getName(), RATINGS))
            {
                assertEquals(7,tbl.getCardinality());
                assertEquals(4,tbl.findColumn("rate").getCardinality());
                assertEquals(7,tbl.findColumn("review").getCardinality());
            }
            else
            {
                fail("Unexpected table " + tbl.getName());
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

    /**
     */
    @Test
    public void testPages() throws Exception
    {
    }
}
