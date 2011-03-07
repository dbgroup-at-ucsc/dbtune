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

import static edu.ucsc.dbtune.core.JdbcConnectionManager.DATABASE;
import static edu.ucsc.dbtune.core.JdbcConnectionManager.DRIVER;
import static edu.ucsc.dbtune.core.JdbcConnectionManager.PASSWORD;
import static edu.ucsc.dbtune.core.JdbcConnectionManager.URL;
import static edu.ucsc.dbtune.core.JdbcConnectionManager.USERNAME;
import static edu.ucsc.dbtune.core.JdbcConnectionManager.makeDatabaseConnectionManager;
import static edu.ucsc.dbtune.spi.EnvironmentProperties.FILE;
import static org.junit.Assert.assertThat;

/**
 * @author huascar.sanchez@gmail.com (Huascar A. Sanchez)
 */
public class IndexExtractorFunctionalTest {
    private static DatabaseConnection connection;

    @BeforeClass
    public static void setUp() throws Exception {
        final Environment environment = Environment.getInstance();
        final Properties  properties  = new Properties(){{
            setProperty(DRIVER,   environment.getJDBCDriver());
            setProperty(URL,      environment.getDatabaseUrl());
            setProperty(DATABASE, environment.getDatabaseName());
            setProperty(USERNAME, environment.getUsername());
            setProperty(PASSWORD, environment.getPassword());
        }};
        final ConnectionManager manager = makeDatabaseConnectionManager(properties);
        connection = manager.connect();
    }

    @Test
    @If(condition = "isDatabaseConnectionAvailable", is = true)
    public void testIndexExtractorNotNull() throws Exception {
        final IndexExtractor extractor = connection.getIndexExtractor();
        assertThat(extractor, CoreMatchers.notNullValue());
    }

    @Test
    @If(condition = "isDatabaseConnectionAvailable", is = true)
    @Ignore // I am getting java.lang.RuntimeException: org.postgresql.util.PSQLException: ERROR: syntax error at or near "RECOMMEND"
    // are we running the correct Postgres version?
    public void testRecommendIndexes() throws Exception {
        final IndexExtractor extractor = connection.getIndexExtractor();
        final File workload = new File(System.getProperty("user.dir") + "/resources/select/" + "workload.sql");
        final Iterable<DBIndex> candidates = extractor.recommendIndexes(workload);
        assertThat(candidates, CoreMatchers.<Object>notNullValue());
    }

    @Test
    @If(condition = "isDatabaseConnectionAvailable", is = true)
    @Ignore // I am getting java.lang.RuntimeException: org.postgresql.util.PSQLException: ERROR: syntax error at or near "RECOMMEND"
    // are we running the correct Postgres version?
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
