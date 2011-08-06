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
package edu.ucsc.dbtune.core;

import edu.ucsc.dbtune.core.metadata.DB2Index;
import edu.ucsc.dbtune.core.metadata.Column;
import edu.ucsc.dbtune.core.metadata.PGIndex;
import edu.ucsc.dbtune.spi.core.Function;
import edu.ucsc.dbtune.spi.core.Functions;
import edu.ucsc.dbtune.spi.core.Parameter;
import edu.ucsc.dbtune.spi.core.Parameters;
import edu.ucsc.dbtune.util.IndexBitSet;
import edu.ucsc.dbtune.util.Instances;
import edu.ucsc.dbtune.util.Iterables;
import edu.ucsc.dbtune.util.Objects;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

import static edu.ucsc.dbtune.core.DBTuneInstances.newDB2Index;
import static edu.ucsc.dbtune.core.DBTuneInstances.newPGIndex;
import static edu.ucsc.dbtune.core.JdbcMocks.makeResultSet;
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
            final WhatIfOptimizer       wio     = each.getWhatIfOptimizer();
            final IBGWhatIfOptimizer    ibgWio  = each.getIBGWhatIfOptimizer();
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
        final IndexExtractor ie = connection.getIndexExtractor();
        connection.close();
        ie.recommendIndexes("SELECT * FROM R;");
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

    @Ignore @Test
    public void testBasicUsageScenario_IBGSpecific_WhatIfOptimizationCostWithProfiledIndex() throws Exception {
        checkIBGWhatIfOptimizerCostWithProfiledIndex(
                new IndexBitSet(),              // index configuration
                new IndexBitSet(),              // used set
                newDB2Index(),                  // profiled index
                connectionManager.connect()     // database connection
        );
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testBasicUsageScenario_IBGSpecific_WhatIfOptimizationCostWithProfiledPGIndex() throws Exception {
        checkIBGWhatIfOptimizerCostWithProfiledIndex(
                new IndexBitSet(),                  // index configuration
                new IndexBitSet(),                  // used set
                newPGIndex(),                       // profiled index
                connectionManager2.connect()        // database connection
        );
    }

    private static <T extends DBIndex> void checkIBGWhatIfOptimizerCostWithProfiledIndex(IndexBitSet configuration,
                                                                                         IndexBitSet usedSet,
                                                                                         T pi,
                                                                                         DatabaseConnection connection
    ) throws SQLException {
        final IBGWhatIfOptimizer wo = connection.getIBGWhatIfOptimizer();
        final double cost = wo.estimateCost("SELECT * FROM R;", configuration, usedSet, pi);
        System.out.println(cost);
        assertThat(Double.compare(cost, 2.0) == 0 || Double.compare(cost, 1.0) == 0, is(true));
        connection.close();
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
            final Iterable<DBIndex> found = Objects.as(each.getIndexExtractor().recommendIndexes("SELECT * FROM R;"));
            assertThat(Iterables.count(found) == 3, is(true));
        }
    }

    @Test
    public void testRecommendIndexesFromWorkloadFileScenario() throws Exception {
        File a = File.createTempFile("workload", ".sql");
        try {
            prepareFile(a);
            // DB2 Connection Manager uses the Advisor, so the idea is to refactor that advisor and
            // make it an actual Object rather a utility class.
            checkRecommendIndexesFromWorkloadFile(a, connectionManager2.connect());
        } finally {
            a.deleteOnExit();
        }
    }

    private static void checkRecommendIndexesFromWorkloadFile(File workload, DatabaseConnection... connections) throws Exception {
        for(DatabaseConnection each : connections){
            final Iterable<DBIndex> found = Objects.as(each.getIndexExtractor().recommendIndexes(workload));
            assertThat(Iterables.count(found) == 3, is(true));
        }
    }

    private static void prepareFile(File file) throws Exception {
        final BufferedWriter out = new BufferedWriter(new FileWriter(file));
        out.write("SELECT * FROM R;");
        out.close();
    }

    @Test
    public void testFixCandidatesScenario() throws Exception {
        final List<DB2Index> db2CandidateSet = Instances.newList();
        db2CandidateSet.add(newDB2Index());
        final List<PGIndex>  pgCandidateSet  = Instances.newList();
        pgCandidateSet.add(newPGIndex());
        checkFixCandidatesScenario(db2CandidateSet, connectionManager.connect());
        checkFixCandidatesScenario(pgCandidateSet, connectionManager2.connect());
    }

    private static void checkFixCandidatesScenario(List<? extends DBIndex> candidateSet, DatabaseConnection connection) throws Exception {
        final IBGWhatIfOptimizer optimizer = connection.getIBGWhatIfOptimizer();
        optimizer.fixCandidates(candidateSet);
        assertThat(Objects.<Iterable<DBIndex>>as(candidateSet), equalTo(Objects.<AbstractIBGWhatIfOptimizer>as(optimizer).getCandidateSet()));
    }

    @Test
    public void testDB2ConnectionAdjustment() throws Exception {
        final ConnectionManager c2 = DBTuneInstances.newDatabaseConnectionManagerWithSwitchOffOnce(
                DBTuneInstances.newDB2Properties()
        );

        final DatabaseConnection d = c2.connect();
        System.out.println(d);
    }

    @SuppressWarnings({"RedundantTypeArguments"})
    private static <T extends DBIndex> void checkExplainInfoScenario(DatabaseConnection connection) throws Exception {
        final WhatIfOptimizer whatIfOptimizer   = connection.getWhatIfOptimizer();
        final List<T>  pgCandidateSet  = Instances.newList();
        final T index1 = Objects.<T>as(newPGIndex(12));
        final T index2 = Objects.<T>as(newPGIndex(21));

        pgCandidateSet.add(index1);
        pgCandidateSet.add(index2);

        final ExplainInfo info = whatIfOptimizer.explain("SELECT * FROM R", pgCandidateSet);

        assertThat(info, notNullValue());
        assertThat(info.isDML(), is(false));
        assertThat(Double.compare(info.getIndexMaintenanceCost(index1), 0) == 0, is(true));

        final Column  col  = new Column(12345);
        final DBIndex idx  = new PGIndex(121112, 3.0, 45.0, "CREATE SYNCHRONIZED INDEX sat_index_121112");
        final DBIndex idx2 = new PGIndex(56789, true, Arrays.asList(col), Arrays.asList(true), 132111, 3.5, 45.0, "CREATE SYNCHRONIZED INDEX sat_index_132111");

        connection.getWhatIfOptimizer().explain("SELECT * FROM R", Arrays.asList(idx, idx2));
    }

    @After
    public void tearDown() throws Exception {
        connectionManager.close();
        connectionManager = null;
        connectionManager2.close();
        connectionManager2 = null;
    }

}
