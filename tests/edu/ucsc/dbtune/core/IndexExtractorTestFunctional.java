package edu.ucsc.dbtune.core;

import edu.ucsc.dbtune.spi.Environment;
import org.hamcrest.CoreMatchers;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Condition;
import org.junit.If;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;
import java.util.Properties;

import static org.junit.Assert.assertThat;

/**
 * @author huascar.sanchez@gmail.com (Huascar A. Sanchez)
 */
public class IndexExtractorTestFunctional {
    private static DatabaseConnection connection;
    private static Environment        environment;

    @BeforeClass
    public static void setUp() throws Exception {
        environment = Environment.getInstance();
        connection  = makeDatabaseConnectionManager(environment.getAll()).connect();
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
        final IndexExtractor    extractor  = connection.getIndexExtractor();
        final File              workload   = new File(environment.getWorkloadFolder() + "/movies/workload.sql");
        final Iterable<DBIndex> candidates = extractor.recommendIndexes(workload);

        assertThat(candidates, CoreMatchers.<Object>notNullValue());
    }

    @Test
    @If(condition = "isDatabaseConnectionAvailable", is = true)
    public void testSingleSQLRecommendIndexes() throws Exception {
        final IndexExtractor extractor = connection.getIndexExtractor();
        final Iterable<DBIndex> candidates = extractor.recommendIndexes("SELECT 1, COUNT(*) FROM tpch.lineitem table0 " +
                "WHERE table0.l_commitdate BETWEEN '1996-06-24-21.19.14.000000' " +
                "AND '1996-07-03-21.19.14.000000';");
        assertThat(candidates, CoreMatchers.<Object>notNullValue());
    }

    @AfterClass
    public static void tearDown() throws Exception{
        connection = null;
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
