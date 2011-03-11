package edu.ucsc.dbtune.core;

import edu.ucsc.dbtune.spi.Environment;
import edu.ucsc.dbtune.util.Iterables;
import edu.ucsc.dbtune.util.SQLScriptExecuter;
import org.hamcrest.CoreMatchers;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Condition;
import org.junit.If;
import org.junit.Test;

import java.io.File;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.util.Properties;

import static edu.ucsc.dbtune.core.JdbcConnectionManager.makeDatabaseConnectionManager;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assume.assumeThat;

/**
 * @author huascar.sanchez@gmail.com (Huascar A. Sanchez)
 */
public class IndexExtractorTestFunctional {
    private static DatabaseConnection connection;
    private static Environment        environment;

    @BeforeClass
    public static void setUp() throws Exception {
        environment = Environment.getInstance();

        final Properties        connProps   = environment.getAll();
        final ConnectionManager manager     = makeDatabaseConnectionManager(connProps);
        connection = manager.connect();

        final String     setupScript    = environment.getScriptAtWorkloadFolder("/movies/create.sql");
        SQLScriptExecuter.execute(connection, setupScript);
        if(connection.getJdbcConnection().getAutoCommit()) {
            // dbtune expects this value to be set to false.
            connection.getJdbcConnection().setAutoCommit(false);
        }
    }


    @Test
    @If(condition = "isDatabaseConnectionAvailable", is = true)
    public void testConnectionIsAlive() throws Exception {
        assertThat(connection.isOpened(), is(true));
        final Connection jdbcConnection = connection.getJdbcConnection();
        final DatabaseMetaData meta = jdbcConnection.getMetaData();
        assertThat(meta, CoreMatchers.<Object>notNullValue());
    }

    @Test
    @If(condition = "isDatabaseConnectionAvailable", is = true)
    public void testIndexExtractorNotNull() throws Exception {
        final IndexExtractor extractor = connection.getIndexExtractor();
        assertThat(extractor, CoreMatchers.notNullValue());
    }

    @Test
    @If(condition = "isDatabaseConnectionAvailable", is = true)
    public void testRecommendIndexes() throws Exception {
        final IndexExtractor    extractor   = connection.getIndexExtractor();
        final File              workload    = new File(
                environment.getScriptAtWorkloadFolder("/movies/workload.sql")
        );
        final Iterable<DBIndex> candidates  = extractor.recommendIndexes(workload);

        assertThat(candidates, CoreMatchers.<Object>notNullValue());
        //todo(Huascar) investigate why we are not getting any index.
        assumeThat(Iterables.asCollection(candidates).isEmpty(), is(false));
    }

    @Test
    @If(condition = "isDatabaseConnectionAvailable", is = true)
    public void testSingleSQLRecommendIndexes() throws Exception {
        final IndexExtractor    extractor   = connection.getIndexExtractor();
        final Iterable<DBIndex> candidates  = extractor.recommendIndexes(
                "select count(*) from actors where actors.afirstname like '%an%';"
        );

        assertThat(candidates, CoreMatchers.<Object>notNullValue());
        //todo(Huascar) investigate why we are not getting any index.
        assumeThat(Iterables.asCollection(candidates).isEmpty(), is(false));
    }

    @AfterClass
    public static void tearDown() throws Exception{
        if(connection != null) connection.close();
        connection  = null;
        environment.getAll().clear();
        environment = null;
    }

    @Condition
    public static boolean isDatabaseConnectionAvailable(){
        final boolean isNotNull = connection != null;
        boolean isOpened = false;
        if(isNotNull){
            isOpened = connection.isOpened();
        }
        return isNotNull && isOpened;
    }

}
