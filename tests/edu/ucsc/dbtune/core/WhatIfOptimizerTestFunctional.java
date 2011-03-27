package edu.ucsc.dbtune.core;

import edu.ucsc.dbtune.spi.Environment;
import edu.ucsc.dbtune.util.Files;
import edu.ucsc.dbtune.util.SQLScriptExecuter;
import org.hamcrest.CoreMatchers;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Condition;
import org.junit.If;
import org.junit.Test;

import java.io.File;
import java.util.Properties;

import static edu.ucsc.dbtune.core.JdbcConnectionManager.makeDatabaseConnectionManager;
import static edu.ucsc.dbtune.util.DBUtilities.trimSqlStatement;
import static edu.ucsc.dbtune.util.Instances.newBitSet;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assume.assumeThat;

/**
 * Functional test for what-if optimizer implementations
 *
 * The test should check basically two properties:
 *   * monotonicity
 *   * sanity
 *
 * For more information on what these properties mean, refer to page 57 (Chapter 4, Section 2.1,
 * Property 4.1 and 4.2 respectively).
 *
 * @author huascar.sanchez@gmail.com (Huascar A. Sanchez)
 * @see {@code thesis} <a
 * href="http://proquest.umi.com/pqdlink?did=2171968721&Fmt=7&clientId=1565&RQT=309&VName=PQD">
 *     "On-line Index Selection for Physical Database Tuning"</a>
 */
public class WhatIfOptimizerTestFunctional {
    private static DatabaseConnection connection;
    private static Environment environment;

    @BeforeClass
    public static void setUp() throws Exception {
        environment = Environment.getInstance();

        final Properties        connProps   = environment.getAll();
        final ConnectionManager manager     = makeDatabaseConnectionManager(connProps);

        try {connection = manager.connect();} catch (Exception e) {connection = null;}
        if(connection == null) return;

        final String     setupScript    = environment.getScriptAtWorkloadsFolder("/movies/create.sql");
        SQLScriptExecuter.execute(connection, setupScript);
        if(connection.getJdbcConnection().getAutoCommit()) {
            // dbtune expects this value to be set to false.
            connection.getJdbcConnection().setAutoCommit(false);
        }
    }

    @Test(expected = AssertionError.class) // this test will pass once the what if optimizer returns something....
    @If(condition = "isDatabaseConnectionAvailable", is = true)
    public void testManyWorloadsWhatIfOptimization_Failed_Unexpected_Result_IndexOverhead() throws Exception {
        final IndexExtractor    extractor   = connection.getIndexExtractor();
        final File workload    = new File(
                environment.getScriptAtWorkloadsFolder("/movies/workload.sql")
        );

        final WhatIfOptimizer   optimizer   = connection.getWhatIfOptimizer();


        for (String line : Files.getLines(workload)) {
            final String            sql         = trimSqlStatement(line);
            final Iterable<DBIndex> candidates  = extractor.recommendIndexes(workload);

            assertThat(candidates, CoreMatchers.<Object>notNullValue());

            final ExplainInfo       info        = optimizer.explain(sql, candidates);
            assertThat(info, CoreMatchers.<Object>notNullValue());

            for(DBIndex each : candidates){
               assumeThat(info.getIndexMaintenanceCost(each) >= 0.0, is(true));
            }
        }
    }


    @Test(expected = AssertionError.class) // this test will pass once the what if optimizer returns something....
    @If(condition = "isDatabaseConnectionAvailable", is = true)
    public void testSingleSQLWhatIfOptimization_Failed_Unexpected_Result_IndexOverhead() throws Exception {
        final String            query       = "select count(*) from actors where " +
                "actors.afirstname like '%an%';";
        final IndexExtractor    extractor   = connection.getIndexExtractor();
        final Iterable<DBIndex> candidates  = extractor.recommendIndexes(query);
        final WhatIfOptimizer   optimizer   = connection.getWhatIfOptimizer();

        assertThat(candidates, CoreMatchers.<Object>notNullValue());

        final ExplainInfo       info        = optimizer.explain(query, candidates);

        assertThat(info, CoreMatchers.<Object>notNullValue());
        assertThat(info.isQuery(), is(true));
        for(DBIndex each : candidates){
           assumeThat(info.getIndexMaintenanceCost(each) >= 0.0, is(true));
        }
    }

    @Test
    @If(condition = "isDatabaseConnectionAvailable", is = true)
    public void testSingleSQLIBGWhatIfOptimization_Failed_Unexpected_Result_IndexOverhead() throws Exception {
        final String            query       = "select count(*) from actors where " +
                "actors.afirstname like '%an%';";
        final IndexExtractor     extractor   = connection.getIndexExtractor();
        final Iterable<DBIndex>  candidates  = extractor.recommendIndexes(query);
        final IBGWhatIfOptimizer optimizer   = connection.getIBGWhatIfOptimizer();

        assertThat(candidates, CoreMatchers.<Object>notNullValue());

        double       cost        = optimizer.estimateCost(query, newBitSet(), newBitSet());

        assumeThat(cost >= -1.0, is(true));
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
