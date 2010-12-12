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
import edu.ucsc.dbtune.core.metadata.PGIndex;
import edu.ucsc.dbtune.spi.core.Command;
import edu.ucsc.dbtune.spi.core.Commands;
import edu.ucsc.dbtune.spi.core.Parameter;
import edu.ucsc.dbtune.spi.core.Parameters;
import edu.ucsc.dbtune.util.DefaultBitSet;
import edu.ucsc.dbtune.util.Instances;
import edu.ucsc.dbtune.util.Objects;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import static edu.ucsc.dbtune.core.DBTuneInstances.newDB2Index;
import static edu.ucsc.dbtune.core.DBTuneInstances.newPGIndex;
import static edu.ucsc.dbtune.core.JdbcMocks.makeResultSet;
import static edu.ucsc.dbtune.util.Instances.str;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * @author huascar.sanchez@gmail.com (Huascar A. Sanchez)
 */
public class DatabasePackageTest {
    private DatabaseConnectionManager<DB2Index> connectionManager;
    private DatabaseConnectionManager<PGIndex>  connectionManager2;

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

    private static void checkIndexExtractors(DatabaseConnection<?>... connections){
        for(DatabaseConnection<?> each : connections){
            assertNotNull(each.getIndexExtractor());
        }
    }

    private static void checkWhatIfOptimizers(DatabaseConnection<?>... connections){
        for(DatabaseConnection<?> each : connections){
            assertNotNull("what-if optimizer is not null", each.getWhatIfOptimizer());
            assertTrue("what-if optimizer has an initial whatIfCount of 0", each.getWhatIfOptimizer().getWhatIfCount() == 0);
        }
    }

    private static void checkConnections(DatabaseConnectionManager<?>... connectionManagers) throws Exception {
        for(DatabaseConnectionManager<?> each : connectionManagers){
            final DatabaseConnection<?> c1 = each.connect();
            final DatabaseConnection<?> c2 = each.connect();
            assertNotNull("connection 1 is not null", c1);
            assertNotNull("connection 2 is not null", c1);
            assertNotSame("both connections are different", c1, c2);
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

    private static void checkIndexExtractorViolation(DatabaseConnection<?> connection) throws Exception {
        final DatabaseIndexExtractor ie = connection.getIndexExtractor();
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

    private static void checkWhatIfOptimizerViolation(DatabaseConnection<?> connection) throws Exception {
        final DatabaseWhatIfOptimizer<?> ie = connection.getWhatIfOptimizer();
        connection.close();
        ie.whatIfOptimize("SELECT * FROM R;");
    }

    @Test
    public void testBasicUsageScenario_WhatIfOptimizationCost() throws Exception {
        // create connections
        // used empty configuration and usedSet bitsets
        checkWhatIfOptimizerCostCalculation(
                new DefaultBitSet(), new DefaultBitSet(),
                connectionManager.connect(), connectionManager2.connect()
        );
    }

    private static void checkWhatIfOptimizerCostCalculation(DefaultBitSet configuration, DefaultBitSet usedSet, DatabaseConnection<?>... connections) throws SQLException {
        for(DatabaseConnection<?> each : connections){
            final DatabaseWhatIfOptimizer<?> wo = each.getWhatIfOptimizer();
            final Double cost = wo.whatIfOptimize("SELECT * FROM R;").using(configuration, usedSet).toGetCost();
            assertTrue("1.0 cost?", Double.compare(cost, 1.0) == 0);
        }
    }

    @Test
    public void testBasicUsageScenario_WhatIfOptimizationCostWithProfiledIndex() throws Exception {
        checkWhatIfOptimizerCostCalculationWithProfiledIndex(new DefaultBitSet(), new DefaultBitSet(), newDB2Index(), connectionManager.connect());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testBasicUsageScenario_WhatIfOptimizationCostWithProfiledPGIndex() throws Exception {
        checkWhatIfOptimizerCostCalculationWithProfiledIndex(new DefaultBitSet(), new DefaultBitSet(), newPGIndex(), connectionManager2.connect());
    }

    private static <T extends DBIndex<T>> void checkWhatIfOptimizerCostCalculationWithProfiledIndex(
            DefaultBitSet configuration, DefaultBitSet usedSet, T pi, DatabaseConnection<T> connection
    ) throws SQLException {
        final DatabaseWhatIfOptimizer<T> wo = connection.getWhatIfOptimizer();
        final Double cost = wo.whatIfOptimize("SELECT * FROM R;").using(configuration, pi, usedSet).toGetCost();
        assertTrue("2.0 cost?", Double.compare(cost, 2.0) == 0);
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

        final Command<Parameter, RuntimeException> a = new Command<Parameter, RuntimeException>(){
            @Override
            public Parameter apply(Parameter input) throws RuntimeException {
                final ResultSet rs = input.getParameterValue(ResultSet.class);
                assertNotNull(rs);
                return Parameters.makeAnonymousParameter(rs);
            }
        };

        final Command<String, RuntimeException> s = new Command<String, RuntimeException>(){
            @Override
            public String apply(Parameter input) throws RuntimeException {
                final ResultSet rs = input.getParameterValue(ResultSet.class);
                assertNotNull(rs);
                return str(rs.getClass().getSimpleName());
            }
        };

        final Command<String, RuntimeException> f = Commands.compose(a, s);
        final String answer = Commands.supplyValue(f, rs);
        assertEquals("we should have the same answer <MockResultSet>", answer, name);
    }


    @Test
    public void testRecommendIndexesFromSQLScenario() throws Exception {
        checkRecommendIndexesFromSQL(connectionManager.connect(), connectionManager2.connect());
    }

    private static void checkRecommendIndexesFromSQL(DatabaseConnection<?>... connections) throws Exception {
        for(DatabaseConnection<?> each : connections){
            final Iterable<DBIndex<?>> found = Objects.as(each.getIndexExtractor().recommendIndexes("SELECT * FROM R;"));
            assertTrue("3 indexes", Instances.count(found) == 3);
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

    private static void checkRecommendIndexesFromWorkloadFile(File workload, DatabaseConnection<?>... connections) throws Exception {
        for(DatabaseConnection<?> each : connections){
            final Iterable<DBIndex<?>> found = Objects.as(each.getIndexExtractor().recommendIndexes(workload));
            assertTrue("3 indexes", Instances.count(found) == 3);
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

    private static <T extends DBIndex<T>> void checkFixCandidatesScenario(List<T> candidateSet, DatabaseConnection<T> connection) throws Exception {
        final DatabaseWhatIfOptimizer<T> optimizer = connection.getWhatIfOptimizer();
        optimizer.fixCandidates(candidateSet);
        assertEquals("same candidate set", Objects.<Iterable<T>>as(candidateSet), Objects.<AbstractDatabaseWhatIfOptimizer<T>>as(optimizer).getCandidateSet());
    }

    @Test
    public void testExplainInfoScenario() throws Exception {
        final DatabaseConnectionManager<PGIndex> c1 = DBTuneInstances.newDatabaseConnectionManagerWithSwitchOffOnce(
                DBTuneInstances.newPGSQLProperties()
        );
        checkExplainInfoScenario(c1.connect());
        //todo(Huascar) write the test for ExplainInfo<PGIndex>
        c1.close();
    }

    @SuppressWarnings({"RedundantTypeArguments"})
    private static <T extends DBIndex<T>> void checkExplainInfoScenario(DatabaseConnection<T> connection) throws Exception {
        final DatabaseWhatIfOptimizer<T> ie   = connection.getWhatIfOptimizer();
        final List<T>  pgCandidateSet  = Instances.newList();
        final T index1 = Objects.<T>as(newPGIndex(12));
        final T index2 = Objects.<T>as(newPGIndex(21));
        pgCandidateSet.add(index1);
        pgCandidateSet.add(index2);
        ie.fixCandidates(pgCandidateSet);        
        final ExplainInfo<T> info = ie.explainInfo("SELECT * FROM R");
        assertNotNull(info);
        assertFalse(info.isDML());
        assertTrue(Double.compare(info.maintenanceCost(index1), 0) == 0);  // since is DML
    }



    @After
    public void tearDown() throws Exception {
        connectionManager.close();
        connectionManager = null;
        connectionManager2.close();
        connectionManager2 = null;
    }

}
