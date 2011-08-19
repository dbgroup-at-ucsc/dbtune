/* ************************************************************************** *
 *   Copyright 2010 University of California Santa Cruz                       *
 *                                                                            *
 *   Licensed under the Apache License, Version 2.0 (the "License");          *
 *   you may not use this file except in compliance with the License.         *
 *   You may obtain a copy of the License at                                  *
 *                                                                            *
 *       http://www.apache.org/licenses/LICENSE-2.0                           *
 *                                                                            *
 *   Unless required by applicable law or agreed to in writing, software      *
 *   distributed under the License is distributed on an "AS IS" BASIS,        *
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. *
 *   See the License for the specific language governing permissions and      *
 *   limitations under the License.                                           *
 * ************************************************************************** */
package edu.ucsc.dbtune.optimizer;

import edu.ucsc.dbtune.DatabaseSystem;
import edu.ucsc.dbtune.metadata.Configuration;
import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.optimizer.PreparedSQLStatement;
import edu.ucsc.dbtune.optimizer.IBGOptimizer;
import edu.ucsc.dbtune.optimizer.Optimizer;
import edu.ucsc.dbtune.spi.Environment;
import edu.ucsc.dbtune.util.Iterables;
import edu.ucsc.dbtune.workload.SQLCategory;
import edu.ucsc.dbtune.workload.SQLStatement;

import org.hamcrest.CoreMatchers;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Condition;
import org.junit.If;
import org.junit.Test;

import java.sql.SQLException;
import java.util.Properties;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assume.assumeThat;
import static edu.ucsc.dbtune.util.SQLScriptExecuter.execute;
import static edu.ucsc.dbtune.spi.EnvironmentProperties.SCHEMA;

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
public class OptimizerFunctionalTest
{
    public static DatabaseSystem db;
    public static Environment    en;

    @BeforeClass
    public static void setUp() throws Exception
    {
        Properties cfg;
        String     ddl;

        cfg = new Properties(Environment.getInstance().getAll());

        cfg.setProperty(SCHEMA,"one_table");

        en  = new Environment(cfg);
        db  = DatabaseSystem.newDatabaseSystem(en);
        ddl = en.getScriptAtWorkloadsFolder("one_table/create.sql");

        {
            // DatabaseSystem reads the catalog as part of its creation, so we need to wipe anything 
            // in the movies schema and reload the data. Then create a DB again to get a fresh 
            // catalog
            //execute(db.getConnection(), ddl);
            db.getConnection().close();

            db = null;
            db = DatabaseSystem.newDatabaseSystem(en);
        }
    }

    @AfterClass
    public static void tearDown() throws Exception{
        db.getConnection().close();
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

    @Condition
    public static boolean isDatabaseConnectionAvailable() throws SQLException {
        return !db.getConnection().isClosed();
    }
}
