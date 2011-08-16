package edu.ucsc.dbtune;

import edu.ucsc.dbtune.connectivity.ConnectionManager;
import edu.ucsc.dbtune.connectivity.DatabaseConnection;
import edu.ucsc.dbtune.metadata.Configuration;
import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.metadata.SQLCategory;
import edu.ucsc.dbtune.optimizer.PreparedSQLStatement;
import edu.ucsc.dbtune.optimizer.IBGOptimizer;
import edu.ucsc.dbtune.optimizer.Optimizer;
import edu.ucsc.dbtune.spi.Environment;
import edu.ucsc.dbtune.util.SQLScriptExecuter;

import org.hamcrest.CoreMatchers;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Condition;
import org.junit.If;
import org.junit.Test;

import java.io.File;
import java.util.Properties;

import static edu.ucsc.dbtune.connectivity.JdbcConnectionManager.makeDatabaseConnectionManager;
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

        File   outputdir   = new File(environment.getOutputFoldername() + "/one_table");
        String ddlfilename = environment.getScriptAtWorkloadsFolder("one_table/create.sql");

        outputdir.mkdirs();
        //SQLScriptExecuter.execute(connection.getJdbcConnection(), ddlfilename);
        connection.getJdbcConnection().setAutoCommit(false);
    }

    @Test // this test will pass once the what if optimizer returns something....
    @If(condition = "isDatabaseConnectionAvailable", is = true)
    public void testSingleSQLWhatIfOptimization() throws Exception {
        final Optimizer   optimizer  = connection.getOptimizer();
        final Configuration candidates = optimizer.recommendIndexes("select a from tbl where a = 5;");

        assertThat(candidates, CoreMatchers.<Object>notNullValue());

        final PreparedSQLStatement info = optimizer.explain("select a from tbl where a = 5;", candidates);

        assertThat(info, CoreMatchers.<Object>notNullValue());
        assertThat(info.getStatement().getSQLCategory().isSame(SQLCategory.QUERY), is(true));
        for(Index each : candidates){
           assumeThat(info.getUpdateCost(each) >= 0.0, is(true));
        }
    }


    @Test
    @If(condition = "isDatabaseConnectionAvailable", is = true)
    public void testSingleSQLIBGWhatIfOptimization() throws Exception {
        final Configuration candidates = connection.getOptimizer().recommendIndexes("select count(*) from tbl where b > 3");
        final IBGOptimizer optimizer = new IBGOptimizer(connection.getOptimizer());

        assertThat(candidates, CoreMatchers.<Object>notNullValue());

        double cost = optimizer.explain("select count(*) from tbl where b > 3").getCost();

        assumeThat(cost >= 0, is(true));
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
