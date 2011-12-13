package edu.ucsc.dbtune.metadata.extraction;

import edu.ucsc.dbtune.metadata.Catalog;
import edu.ucsc.dbtune.metadata.Column;
import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.metadata.Schema;
import edu.ucsc.dbtune.metadata.Table;
import edu.ucsc.dbtune.util.Environment;
import edu.ucsc.dbtune.util.Strings;

import java.sql.Connection;

import org.junit.BeforeClass;
import org.junit.Test;

import static edu.ucsc.dbtune.DatabaseSystem.newConnection;
import static edu.ucsc.dbtune.DatabaseSystem.newExtractor;
import static edu.ucsc.dbtune.metadata.DatabaseObject.NON_ID;
import static edu.ucsc.dbtune.util.SQLScriptExecuter.execute;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;

/**
 * Test for the metadata extraction functionality. This test assumes that the system on the backend 
 * passes the {@link GenericJDBCExtractorTestFunctional} test.
 * <p>
 * This test executes all the tests for which {@link GenericJDBCExtractorTest} relies on 
 * DBMS-specific mocks (eg. classes contained in {@link java.sql}).
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
        assertThat(cat.size() > 0, is(true));

        assertThat(cat.find("movies"), is(notNullValue()));
    }

    /**
     * Tests ids aren't {@link DatabaseObject#NON_ID}, i.e. ids are correctly assigned
     */
    @Test
    public void testIDs() throws Exception
    {
        assertThat(cat.getInternalID(), is(1));

        for (Schema sch : cat)
        {
            assertThat(sch.getInternalID(), is(not(NON_ID)));

            for (Table tbl : sch.tables())
            {
                assertThat(tbl.getInternalID(), is(not(NON_ID)));

                for (Column col : tbl.columns())
                {
                    assertThat(col.getInternalID(), is(not(NON_ID)));
                }

            }

            for (Index idx : sch.indexes())
            {
                assertThat(idx.getInternalID(), is(not(NON_ID)));
            }
        }
    }

    /**
     * Tests tables and columns exist
     */
    @Test
    public void testTablesAndColumnsExist() throws Exception
    {
        int expected;

        Schema sch = cat.<Schema>findByName("movies");

        assertThat(cat.size() >= 1, is(true));
        assertThat(sch.size()-sch.getBaseConfiguration().size(), is(8));

        for (Table tbl : sch.tables())
        {
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

            assertThat(tbl.size(), is(expected));
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
        String expected = "";

        for (Table tbl : cat.<Schema>findByName("movies").tables())
        {
            int i = 0;

            for (Column col : tbl.columns())
            {
                if (Strings.same(tbl.getName(), USERS))
                    if (i == 0)
                        expected = "userid";
                    else if (i == 1)
                        expected = "email";
                    else if (i == 2)
                        expected = "password";
                    else if (i == 3)
                        expected = "ufirstname";
                    else if (i == 4)
                        expected = "ulastname";
                    else
                        fail("Unexpected column " + tbl.at(i));

                else if (Strings.same(tbl.getName(), MOVIES))
                    if (i == 0)
                        expected = "movieid";
                    else if (i == 1)
                        expected = "title";
                    else if (i == 2)
                        expected = "yearofr";
                    else if (i == 3)
                        expected = "summary";
                    else if (i == 4)
                        expected = "url";
                    else
                        fail("Unexpected column " + tbl.at(i));

                else if (Strings.same(tbl.getName(), ACTORS))
                    if (i == 0)
                        expected = "aid";
                    else if (i == 1)
                        expected = "afirstname";
                    else if (i == 2)
                        expected = "alastname";
                    else if (i == 3)
                        expected = "dateofb";
                    else
                        fail("Unexpected column " + tbl.at(i));

                else if (Strings.same(tbl.getName(), CREDITCARDS))
                    if (i == 0)
                        expected = "userid";
                    else if (i == 1)
                        expected = "creditnum";
                    else if (i == 2)
                        expected = "credittype";
                    else if (i == 3)
                        expected = "expdate";
                    else
                        fail("Unexpected column " + tbl.at(i));
                else if (Strings.same(tbl.getName(), GENRES))
                    if (i == 0)
                        expected = "mgenre";
                    else if (i == 1)
                        expected = "movieid";
                    else
                        fail("Unexpected column " + tbl.at(i));
                else if (Strings.same(tbl.getName(), CASTS))
                    if (i == 0)
                        expected = "aid";
                    else if (i == 1)
                        expected = "movieid";
                    else
                        fail("Unexpected column " + tbl.at(i));
                else if (Strings.same(tbl.getName(), QUEUE))
                    if (i == 0)
                        expected = "userid";
                    else if (i == 1)
                        expected = "movieid";
                    else if (i == 2)
                        expected = "position";
                    else if (i == 3)
                        expected = "times";
                    else
                        fail("Unexpected column " + tbl.at(i));
                else if (Strings.same(tbl.getName(), RATINGS))
                    if (i == 0)
                        expected = "userid";
                    else if (i == 1)
                        expected = "movieid";
                    else if (i == 2)
                        expected = "rate";
                    else if (i == 3)
                        expected = "review";
                    else
                        fail("Unexpected column " + tbl.at(i));
                else
                    fail("Unexpected table " + tbl.getName());

                assertThat(col.getName(), is(expected));
                i++;
            }
        }
    }

    /**
     * Tests indexes exist
     */
    @Test
    public void testIndexesExist() throws Exception
    {
        assertThat(cat.<Index>findByName("movies.users_userid_email"), is(notNullValue()));
        assertThat(cat.<Index>findByName("movies.creditcards_creditnum_userid_credittype"), is(notNullValue()));
        assertThat(cat.<Index>findByName("movies.movies_moiveid_title_yearofr"), is(notNullValue()));
        assertThat(cat.<Index>findByName("movies.actors_afirstname_alastname_dateofb"), is(notNullValue()));
        assertThat(cat.<Index>findByName("movies.queue_times"), is(notNullValue()));
    }

    /**
     */
    @Test
    public void testPrimaryKeys() throws Exception
    {
        // XXX
    }

    /**
     */
    @Test
    public void testForeignKeys() throws Exception
    {
        // XXX
    }

    /**
     */
    @Test
    public void testColumnConstraints() throws Exception
    {
        // XXX: for columns, check unique, default and not null constraints
    }

    /**
     */
    @Test
    public void testIndexConstraints() throws Exception
    {
        // XXX: for indexes, check asc/desc, default and not null constraints
    }

    /**
     * Tests cardinality of database objects. Checks that tables, columns and indexes have the 
     * expected cardinality. For columns and indexes, this corresponds to the count of unique 
     * entries; for tables, the number of rows contained in it.
     */
    @Test
    public void testCardinality() throws Exception
    {
        // XXX: complete for index cardinality
        for (Table tbl : cat.<Schema>findByName("movies").tables())
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
        // XXX:
    }

    /**
     */
    @Test
    public void testPages() throws Exception
    {
        // XXX:
    }
}
