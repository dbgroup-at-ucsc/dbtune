package edu.ucsc.dbtune.workload;

import java.sql.Connection;
import java.sql.Statement;

import edu.ucsc.dbtune.util.Environment;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static edu.ucsc.dbtune.DatabaseSystem.newConnection;

import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

/**
 * Unit test for the {@link SQLStatement} class.
 *
 * @author Ivo Jimenez
 */
public class QueryLogReaderFunctionalTest
{
    private static Environment env;

    /**
     * @throws Exception
     *      if the workload can't be loaded
     */
    @BeforeClass
    public static void beforeClass() throws Exception
    {
        env = Environment.getInstance();
    }

    /**
     * @throws Exception
     *      if the connection can't be closed
     */
    @AfterClass
    public static void afterClass() throws Exception
    {
    }

    /**
     * checks that a character stream containing multi-line statements is parsed correctly.
     *
     * @throws Exception
     *     if I/O error
     */
    @Test
    public void testWorkloadReading() throws Exception
    {
        Connection con1 = newConnection(env);
        Connection con2 = newConnection(env);

        QueryLogReader reader = QueryLogReader.newQueryLogReader(con1);

        Statement stmt = con2.createStatement();

        // wait for the watcher to read whatever is in the log
        Thread.sleep(11000);

        // get the number of statements that were in the log
        int stmtsBefore = reader.getStatements().size();

        // execute 3 new queries
        stmt.executeQuery("SELECT count(*) from one_table.tbl");
        stmt.executeQuery("SELECT count(*) from movies.actors");
        stmt.executeQuery("SELECT count(*) from movies.movies");

        stmt.close();
        con2.close();

        // wait for the watcher to read again
        Thread.sleep(11000);

        // at least 3 more queries should be on the log
        assertThat(reader.getStatements().size(), is(greaterThanOrEqualTo(stmtsBefore + 3)));

        con1.close();
    }
}
