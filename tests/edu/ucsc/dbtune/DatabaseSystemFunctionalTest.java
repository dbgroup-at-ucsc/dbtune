package edu.ucsc.dbtune;

import org.junit.Test;

import static org.junit.Assert.assertThat;
import static org.hamcrest.CoreMatchers.is;

/**
 * Functional test for the {@link DatabaseSystem} class.
 * <p>
 * This test executes all the tests for which {@link DatabaseSystemTest} relies on DBMS-specific 
 * mocks (eg. classes contained in {@link java.sql}).
 *
 * @author Ivo Jimenez
 */
public class DatabaseSystemFunctionalTest
{
    /**
     * Checks if a system is constructed correctly. This test depends on the contents of the default 
     * configuration file read by {@link Environment}
     *
     * @see Environment
     */
    @Test
    public void testDBConnection() throws Exception
    {
        DatabaseSystem db = DatabaseSystem.newDatabaseSystem();

        assertThat(db.getConnection() != null, is(true));
        assertThat(db.getOptimizer() != null, is(true));
        assertThat(db.getCatalog() != null, is(true));

        db.getConnection().close();
    }
}
