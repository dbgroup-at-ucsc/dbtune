package edu.ucsc.dbtune.optimizer;

import edu.ucsc.dbtune.DatabaseSystem;
import edu.ucsc.dbtune.metadata.Configuration;
import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.optimizer.PreparedSQLStatement;
import edu.ucsc.dbtune.optimizer.IBGOptimizer;
import edu.ucsc.dbtune.optimizer.Optimizer;
import edu.ucsc.dbtune.spi.Environment;
import edu.ucsc.dbtune.util.SQLScriptExecuter;
import edu.ucsc.dbtune.util.Iterables;
import edu.ucsc.dbtune.workload.SQLCategory;
import edu.ucsc.dbtune.workload.SQLStatement;

import org.hamcrest.CoreMatchers;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Condition;
import org.junit.If;
import org.junit.Test;

import java.io.File;
import java.sql.SQLException;

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
 * @author Huascar A. Sanchez
 * @author Ivo Jimenez
 * @see {@code thesis} <a
 * href="http://proquest.umi.com/pqdlink?did=2171968721&Fmt=7&clientId=1565&RQT=309&VName=PQD">
 *     "On-line Index Selection for Physical Database Tuning"</a>
 */
public class WhatIfOptimizerTestFunctional {
    private static DatabaseSystem db;
    private static Environment    en;

    @BeforeClass
    public static void setUp() throws Exception {
        en = Environment.getInstance();
        db = new DatabaseSystem();

        File   outputdir   = new File(en.getOutputFoldername() + "/one_table");
        String ddlfilename = en.getScriptAtWorkloadsFolder("one_table/create.sql");

        outputdir.mkdirs();
        //SQLScriptExecuter.execute(connection.getJdbcConnection(), ddlfilename);
    }

    @Test // this test will pass once the what if optimizer returns something....
    @If(condition = "isDatabaseConnectionAvailable", is = true)
    public void testSingleSQLWhatIfOptimization() throws Exception {
        final Optimizer   optimizer  = db.getOptimizer();
        final Configuration candidates = optimizer.recommendIndexes(new SQLStatement("select a from tbl where a = 5;"));

        assertThat(candidates, CoreMatchers.<Object>notNullValue());

        final PreparedSQLStatement info = optimizer.explain(new SQLStatement("select a from tbl where a = 5;"), candidates);

        assertThat(info, CoreMatchers.<Object>notNullValue());
        assertThat(info.getStatement().getSQLCategory().isSame(SQLCategory.QUERY), is(true));
        for(Index each : candidates){
           assumeThat(info.getUpdateCost(each) >= 0.0, is(true));
        }
    }


    @Test
    @If(condition = "isDatabaseConnectionAvailable", is = true)
    public void testSingleSQLIBGWhatIfOptimization() throws Exception {
        final Configuration candidates = db.getOptimizer().recommendIndexes(new SQLStatement("select count(*) from tbl where b > 3"));
        final IBGOptimizer optimizer = new IBGOptimizer(db.getOptimizer());

        assertThat(candidates, CoreMatchers.<Object>notNullValue());

        double cost = optimizer.explain(new SQLStatement("select count(*) from tbl where b > 3")).getCost();

        assumeThat(cost >= 0, is(true));
    }

    @Test
    @If(condition = "isDatabaseConnectionAvailable", is = true)
    public void testConnectionIsAlive() throws Exception {
        assertThat(db.getConnection().isClosed(), is(false));
    }

    @Test
    @If(condition = "isDatabaseConnectionAvailable", is = true)
    public void testSingleSQLRecommendIndexes() throws Exception {
        Configuration candidates = db.getOptimizer().recommendIndexes(new SQLStatement("select a from tbl where a = 5;"));

        assertThat(candidates, CoreMatchers.<Object>notNullValue());
        assertThat(Iterables.asCollection(candidates).isEmpty(), is(false));

        candidates = db.getOptimizer().recommendIndexes(new SQLStatement("update tbl set a=-1 where a = 5;"));

        assertThat(candidates, CoreMatchers.<Object>notNullValue());
        assertThat(Iterables.asCollection(candidates).isEmpty(), is(false));
    }

    @AfterClass
    public static void tearDown() throws Exception{
        db.getConnection().close();
    }

    @Condition
    public static boolean isDatabaseConnectionAvailable() throws SQLException {
        return !db.getConnection().isClosed();
    }
}
