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
package edu.ucsc.dbtune.spi.ibg;

import edu.ucsc.dbtune.core.DBTuneInstances;
import edu.ucsc.dbtune.core.DBIndex;
import edu.ucsc.dbtune.core.DatabaseConnection;
import edu.ucsc.dbtune.core.DatabaseConnectionManager;
import edu.ucsc.dbtune.core.JdbcConnectionFactory;
import edu.ucsc.dbtune.core.JdbcDatabaseConnectionManager;
import edu.ucsc.dbtune.core.JdbcMocks;
import edu.ucsc.dbtune.core.metadata.PGIndex;
import edu.ucsc.dbtune.ibg.CandidatePool;
import edu.ucsc.dbtune.ibg.ThreadIBGAnalysis;
import edu.ucsc.dbtune.ibg.ThreadIBGConstruction;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

import static edu.ucsc.dbtune.core.JdbcMocks.makeMockPreparedStatement;
import static edu.ucsc.dbtune.core.JdbcMocks.makeMockStatement;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * @author huascar.sanchez@gmail.com (Huascar A. Sanchez)
 */
public class WorkloadHandlingTest {
    DatabaseConnectionManager connectionManager;
    @Before
    public void setUp() throws Exception {
        connectionManager = makeDBConnectionManager(
                new Properties(){
                    {
                        setProperty(JdbcDatabaseConnectionManager.URL, "");
                        setProperty(JdbcDatabaseConnectionManager.USERNAME, "newo");
                        setProperty(JdbcDatabaseConnectionManager.PASSWORD, "hahaha");
                        setProperty(JdbcDatabaseConnectionManager.DATABASE, "matrix");
                        setProperty(JdbcDatabaseConnectionManager.DRIVER, "org.postgresql.Driver");
                    }
                },
                makeJdbcConnectionFactoryWithSwitchOffOn()
        );
    }

    @Test
    public void testBasicInitializationOfWorkloadProfiler() throws Exception {
        final AtomicBoolean               doneWithAnalysis      = new AtomicBoolean(false);
        final AtomicBoolean               doneWithConstruction  = new AtomicBoolean(false);
        final DatabaseConnection connection            = connectionManager.connect();
        final CandidatePool<PGIndex>      pool                  = new CandidatePool<PGIndex>(){{
            addIndex(DBTuneInstances.newPGIndex(1234567890));
        }};
        final ThreadIBGAnalysis analysisPhase         = new ThreadIBGAnalysis(){
            @Override
            public void run() {
                doneWithAnalysis.set(true);
            }
        };
        final ThreadIBGConstruction constructioPhase      = new ThreadIBGConstruction(){
            @Override
            public void run() {
                doneWithConstruction.set(true);
            }
        };
        final WorkloadProfiler<PGIndex>   profiler              = makeWorkloadProfiler(
                connection,
                pool,
                analysisPhase,
                constructioPhase
        );

        assertNotNull(profiler);
        assertTrue(doneWithAnalysis.get());
        assertTrue(doneWithConstruction.get());

    }


    @Test
    public void testProcessQuery() throws Exception {
        final DatabaseConnection connection            = connectionManager.connect();
        final CandidatePool<PGIndex>      pool                  = new CandidatePool<PGIndex>(){{
            addIndex(DBTuneInstances.newPGIndex(1234567890));
        }};
        final WorkloadProfiler<PGIndex>   profiler              = makeWorkloadProfiler(
                connection,
                pool,
                new ThreadIBGAnalysis(){public void run(){} public void waitUntilDone(){}},
                new ThreadIBGConstruction(){public void run(){} public void waitUntilDone(){}}
        );
        ProfiledQuery<PGIndex> p = profiler.processQuery("SELECT * FROM R;");
        assertNotNull(p);
        assertEquals(p.getSQL(), "SELECT * FROM R;");
        assertTrue(p.getWhatIfCount() == 2);
        assertTrue(Double.compare(p.getIndexBenefitGraph().emptyCost(), 1.0) == 0);
    }

    @Test
    public void testAddCandidateFunctionality() throws Exception {
        final DatabaseConnection connection            = connectionManager.connect();
        final CandidatePool<PGIndex>      pool                  = new CandidatePool<PGIndex>(){{
            addIndex(DBTuneInstances.newPGIndex(1234567890));
        }};
        final WorkloadProfiler<PGIndex>   profiler              = makeWorkloadProfiler(
                connection,
                pool,
                new ThreadIBGAnalysis(){public void run(){} public void waitUntilDone(){}},
                new ThreadIBGConstruction(){public void run(){} public void waitUntilDone(){}}
        );

        final CandidatePool.Snapshot<PGIndex> s = profiler.addCandidate(DBTuneInstances.newPGIndex(9876543, 653434));
        assertNotNull(s);
        //todo(Huascar) confirm this later.
        assertNotNull(s.findIndexId(1));
    }

    @Test
    public void testCastingVotes() throws Exception {
        final DatabaseConnection connection            = connectionManager.connect();
        final CandidatePool<PGIndex>      pool                  = new CandidatePool<PGIndex>(){{
            addIndex(DBTuneInstances.newPGIndex(1234567890));
        }};
        final WorkloadProfiler<PGIndex>   profiler              = makeWorkloadProfiler(
                connection,
                pool,
                new ThreadIBGAnalysis(){public void run(){} public void waitUntilDone(){}},
                new ThreadIBGConstruction(){public void run(){} public void waitUntilDone(){}}
        );

        final CandidatePool.Snapshot<PGIndex> s = profiler.processVote(DBTuneInstances.newPGIndex(1234567890), true);
        assertNotNull(s);
        System.out.println(s);
        //todo(Huascar) confirm if this is the right result.
        assertTrue(s.maxInternalId() == 1 || s.maxInternalId() == 0);

    }

    private static WorkloadProfiler<PGIndex> makeWorkloadProfiler(
            DatabaseConnection connection,
            CandidatePool<PGIndex> pool,
            ThreadIBGAnalysis analysisPhase,
            ThreadIBGConstruction consPhase
    ){
        return new WorkloadProfilerImpl<PGIndex>(connection, pool, analysisPhase, consPhase, true);
    }

    @After
    public void tearDown() throws Exception {
        connectionManager.close();
        connectionManager = null;
    }

    private static <I extends DBIndex> DatabaseConnectionManager makeDBConnectionManager(Properties props, JdbcConnectionFactory factory) throws Exception {
        return JdbcDatabaseConnectionManager.makeDatabaseConnectionManager(props, factory);
    }

    private static JdbcConnectionFactory makeJdbcConnectionFactoryWithSwitchOffOn(){
        return new JdbcConnectionFactory(){
            @Override
            public Connection makeConnection(String url, String driverClass, String username, String password, boolean autoCommit) throws SQLException {
                final JdbcMocks.MockConnection conn = new JdbcMocks.MockConnection();
                conn.register(
                        makeMockStatement(true, true, conn),
                        makeMockPreparedStatement(true, true, conn)
                );
                return conn;
            }
        };
    }
}
