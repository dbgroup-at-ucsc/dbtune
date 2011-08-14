/*
 * ****************************************************************************
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
 *  ****************************************************************************
 */
package edu.ucsc.dbtune;

import edu.ucsc.dbtune.advisor.CandidateIndexExtractor;
import edu.ucsc.dbtune.connectivity.ConnectionManager;
import edu.ucsc.dbtune.connectivity.DatabaseConnection;
import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.metadata.SQLCategory;
import edu.ucsc.dbtune.optimizer.Optimizer;
import edu.ucsc.dbtune.optimizer.IBGWhatIfOptimizer;
import edu.ucsc.dbtune.spi.core.Function;
import edu.ucsc.dbtune.spi.core.Functions;
import edu.ucsc.dbtune.spi.core.Parameter;
import edu.ucsc.dbtune.spi.core.Parameters;
import edu.ucsc.dbtune.util.IndexBitSet;
import edu.ucsc.dbtune.util.Iterables;
import edu.ucsc.dbtune.util.Objects;
import edu.ucsc.dbtune.workload.SQLStatement;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.sql.ResultSet;
import java.sql.SQLException;

import static edu.ucsc.dbtune.JdbcMocks.makeResultSet;
import static edu.ucsc.dbtune.util.Strings.str;
import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;

/**
 * @author huascar.sanchez@gmail.com (Huascar A. Sanchez)
 */
public class DatabasePackageTest {
    private ConnectionManager connectionManager;
    private ConnectionManager connectionManager2;

    @Before
    public void setUp() throws Exception {
        connectionManager  = DBTuneInstances.newDB2DatabaseConnectionManager();
        connectionManager2 = DBTuneInstances.newPGDatabaseConnectionManager();
    }


    @Test
    public void testBasicUsageScenario_CreatingConnections() throws Exception {
        // create two connections
        // check that neither one is null
        // check that both connections represent diff instances.
        checkConnections(connectionManager, connectionManager2);
    }

    @Test
    public void testBasicUsageScenario_RetrievingIndexExtractors() throws Exception {
        // create two connections (different drivers)
        // check that each connection has a non-null index extractor
        checkIndexExtractors(connectionManager.connect(), connectionManager2.connect());
    }

    @Test
    public void testBasicUsageScenario_RetrievingWhatIfOptimizer() throws Exception {
        // create two connections (different drivers)
        // check that each connection has a non-null what if optimizer
        // check that each what if optimizer has its whatIfCount equated to Zero.
        checkWhatIfOptimizers(connectionManager.connect(), connectionManager2.connect());
    }

    private static void checkIndexExtractors(DatabaseConnection... connections){
        for(DatabaseConnection each : connections){
            assertThat(each.getIndexExtractor(), notNullValue());
        }
    }

    private static void checkWhatIfOptimizers(DatabaseConnection... connections){
        for(DatabaseConnection each : connections){
            final Optimizer          wio    = each.getOptimizer();
            final IBGWhatIfOptimizer ibgWio = each.getIBGWhatIfOptimizer();
            assertThat(wio, notNullValue());
            assertThat(ibgWio, notNullValue());
            assertThat(ibgWio.getWhatIfCount(), is(0));
        }
    }

    private static void checkConnections(ConnectionManager... connectionManagers) throws Exception {
        for(ConnectionManager each : connectionManagers){
            final DatabaseConnection c1 = each.connect();
            final DatabaseConnection c2 = each.connect();
            assertThat(c1, notNullValue());
            assertThat(c2, notNullValue());
            assertThat(c1, not(equalTo(c2)));
        }
    }

    @Test(expected = SQLException.class)
    public void testBasicUsageScenario_UsingIndexExtractorWithClosedConnection() throws Exception {
        try {
            checkIndexExtractorViolation(connectionManager.connect());
        } catch (SQLException e) {
            checkIndexExtractorViolation(connectionManager2.connect());
        }

        fail("failed if it got here.");
    }

    private static void checkIndexExtractorViolation(DatabaseConnection connection) throws Exception {
        final CandidateIndexExtractor ie = connection.getIndexExtractor();
        connection.close();
        ie.recommendIndexes(new SQLStatement(SQLCategory.QUERY, "SELECT * FROM R;"));
    }


    @Test(expected = SQLException.class)
    public void testBasicUsageScenario_UsingWhatIfOptimizerWithClosedConnection() throws Exception {
        try {
            checkWhatIfOptimizerViolation(connectionManager.connect());
        } catch (SQLException e) {
            checkWhatIfOptimizerViolation(connectionManager2.connect());
        }

        fail("failed if it got here.");
    }

    private static void checkWhatIfOptimizerViolation(DatabaseConnection connection) throws Exception {
        final IBGWhatIfOptimizer ie = connection.getIBGWhatIfOptimizer();
        connection.close();
        ie.explain("SELECT * FROM R;");
    }

    @Ignore @Test
    public void testBasicUsageScenario_IBGSpecific_EstimatedCost() throws Exception {
        // create connections
        // used empty configuration and usedSet bitsets
        checkIBGWhatIfOptimizerCost(
                new IndexBitSet(), new IndexBitSet(),
                connectionManager.connect(), connectionManager2.connect()
        );
    }

    private static void checkIBGWhatIfOptimizerCost(IndexBitSet configuration,
                                                    IndexBitSet usedSet,
                                                    DatabaseConnection... connections) throws SQLException
    {
        for(DatabaseConnection each : connections){
            final IBGWhatIfOptimizer ibgWhatIfOptimizer = each.getIBGWhatIfOptimizer();
            final double cost = ibgWhatIfOptimizer.estimateCost("SELECT * FROM R;", configuration, usedSet);
            assertThat(Double.compare(cost, 1.0) == 0, is(true));
        }
    }

    @Test
    public void testSharingResultSet() throws Exception {
        // create a parameter result set
        // retrieve the value and check that is not null
        // share the result set across commands
        // check that we are dealing with the same type of result set.
        checkSharedResultSet(Parameters.makeAnonymousParameter(makeResultSet()));
    }


    private static void checkSharedResultSet(Parameter input) throws Exception {
        final ResultSet rs    = input.getParameterValue(ResultSet.class);
        assertNotNull("result set should not be null", rs);
        final String name = rs.getClass().getSimpleName();

        final Function<Parameter, RuntimeException> a = new Function<Parameter, RuntimeException>(){
            @Override
            public Parameter apply(Parameter input) throws RuntimeException {
                final ResultSet rs = input.getParameterValue(ResultSet.class);
                assertThat(rs, notNullValue());
                return Parameters.makeAnonymousParameter(rs);
            }
        };

        final Function<String, RuntimeException> s = new Function<String, RuntimeException>(){
            @Override
            public String apply(Parameter input) throws RuntimeException {
                final ResultSet rs = input.getParameterValue(ResultSet.class);
                assertThat(rs, notNullValue());
                return str(rs.getClass().getSimpleName());
            }
        };

        final Function<String, RuntimeException> f = Functions.compose(a, s);
        final String answer = Functions.supplyValue(f, rs);
        assertThat(answer, equalTo(name));
    }


    @Test
    public void testRecommendIndexesFromSQLScenario() throws Exception {
        checkRecommendIndexesFromSQL(connectionManager.connect(), connectionManager2.connect());
    }

    private static void checkRecommendIndexesFromSQL(DatabaseConnection... connections) throws Exception {
        for(DatabaseConnection each : connections){
            final Iterable<Index> found = Objects.as(each.getIndexExtractor().recommendIndexes(new SQLStatement(SQLCategory.QUERY, "SELECT * FROM R;")));
            assertThat(Iterables.count(found) == 3, is(true));
        }
    }

    @Test
    public void testDB2ConnectionAdjustment() throws Exception {
        final ConnectionManager c2 = DBTuneInstances.newDatabaseConnectionManagerWithSwitchOffOnce(
                DBTuneInstances.newDB2Properties()
        );

        final DatabaseConnection d = c2.connect();
        System.out.println(d);
    }

    @After
    public void tearDown() throws Exception {
        connectionManager.close();
        connectionManager = null;
        connectionManager2.close();
        connectionManager2 = null;
    }
}
