package edu.ucsc.dbtune.workload;

import java.sql.SQLException;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import static org.hamcrest.CoreMatchers.is;

import static org.junit.Assert.assertThat;

/**
 * @author Ivo Jimenez
 */
public class ObservableWorkloadReaderTest
{
    private static int control;

    /** checks the constructor. */
    @Test
    public void testConstructor()
    {
        TestableWorkloadReader wr = new TestableWorkloadReader();

        // this indicates that one new query should be added
        control = 1;

        // let's wait for the watcher to do the update
        try {
            Thread.sleep(11000);
        } catch (InterruptedException ex) {
            throw new RuntimeException(ex);
        }

        assertThat(wr.getStatements().size(), is(1));
        assertThat(wr.getStatements().get(0).getSQL(), is("SELECT control FROM foo"));

        // let's put another one
        try {
            Thread.sleep(11000);
        } catch (InterruptedException ex) {
            throw new RuntimeException(ex);
        }

        assertThat(wr.getStatements().size(), is(2));
        assertThat(wr.getStatements().get(1).getSQL(), is("SELECT control FROM foo"));

        // no new statement should be added
        control = -1;

        try {
            Thread.sleep(11000);
        } catch (InterruptedException ex) {
            throw new RuntimeException(ex);
        }

        assertThat(wr.getStatements().size(), is(2));
        assertThat(wr.getStatements().get(1).getSQL(), is("SELECT control FROM foo"));
    }

    /**
     * just to test.
     */
    public static class TestableWorkloadReader extends ObservableWorkloadReader
    {
        /**
         * just to test.
         */
        public TestableWorkloadReader()
        {
            super(new Workload("test"));
            sqls = new ArrayList<SQLStatement>();
            startWatcher();
        }

        /**
         * {@inheritDoc}
         */
        protected List<SQLStatement> hasNewStatement() throws SQLException
        {
            List<SQLStatement> newStmts = new ArrayList<SQLStatement>();

            if (control > 0)
                newStmts.add(
                    new SQLStatement("SELECT control FROM foo", getWorkload(), control));

            return newStmts;
        }
    }
}
