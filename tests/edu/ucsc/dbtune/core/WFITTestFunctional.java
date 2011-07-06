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
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied  *
 *   See the License for the specific language governing permissions and      *
 *   limitations under the License.                                           *
 * ************************************************************************** */
package edu.ucsc.dbtune.core;

import edu.ucsc.dbtune.advisor.DynamicIndexSet;
import edu.ucsc.dbtune.advisor.IndexPartitions;
import edu.ucsc.dbtune.advisor.KarlsHotsetSelector;
import edu.ucsc.dbtune.advisor.KarlsInteractionSelector;
import edu.ucsc.dbtune.advisor.KarlsWFALog;
import edu.ucsc.dbtune.advisor.KarlsWorkFunctionAlgorithm;
import edu.ucsc.dbtune.advisor.ProfiledQuery;
import edu.ucsc.dbtune.advisor.StaticIndexSet;
import edu.ucsc.dbtune.advisor.WorkloadProfiler;
import edu.ucsc.dbtune.advisor.WorkloadProfilerImpl;
import edu.ucsc.dbtune.ibg.CandidatePool;
import edu.ucsc.dbtune.ibg.CandidatePool.Snapshot;
import edu.ucsc.dbtune.spi.Environment;
import edu.ucsc.dbtune.spi.core.Console;
import edu.ucsc.dbtune.util.DBUtilities;
import edu.ucsc.dbtune.util.Files;
import edu.ucsc.dbtune.util.IndexBitSet;
import edu.ucsc.dbtune.util.SQLScriptExecuter;
import edu.ucsc.dbtune.util.StopWatch;
import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static edu.ucsc.dbtune.core.JdbcConnectionManager.makeDatabaseConnectionManager;
import static edu.ucsc.dbtune.util.Strings.str;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

/**
 * Functional test for the WFIT use case.
 *
 * Some of the documentation contained in this class refers to sections of Karl Schnaitter's 
 * Doctoral Thesis "On-line Index Selection for Physical Database Tuning". Specifically to chapter 
 * 6.
 *
 * @see <a href="http://proquest.umi.com/pqdlink?did=2171968721&Fmt=7&clientId=1565&RQT=309&VName=PQD">
 *     "On-line Index Selection for Physical Database Tuning"</a>
 * @author ivo@cs.ucsc.edu (Ivo Jimenez)
 */
public class WFITTestFunctional
{
    public final static DatabaseConnection connection;
    public final static Environment        env       = Environment.getInstance();

    static {
        try {
            connection = makeDatabaseConnectionManager(env.getAll()).connect();
        } catch(SQLException ex) {
            throw new ExceptionInInitializerError(ex);
        }
    }

    @BeforeClass
    public static void setUp() throws Exception
    {
        File   outputdir   = new File(env.getOutputFoldername() + "/one_table");
        String ddlfilename = env.getScriptAtWorkloadsFolder("one_table/create.sql");

        outputdir.mkdirs();
        SQLScriptExecuter.execute(connection.getJdbcConnection(), ddlfilename);
        connection.getJdbcConnection().setAutoCommit(false);
    }

    @AfterClass
    public static void tearDown() throws Exception
    {
    }

    /**
     * The test has to check first just one query and one index. We need to make sure that the index 
     * gets recommended and dropped consistently to what we expect.
     */
    @Test
    public void testWFIT() throws Exception
    {
        // ignore task's steady-state execution profile, and focus only on the initial performance
        // of WFIT algorithm execution. This is done via a warm up phase.
        // warm up the hotspot compiler, so that we can run this like we
        // were in a live execution (JUnit by default prevents this)
        //testWfitAlgorithm(0);

        // real execution
        testWfitAlgorithm(1);
    }

    /**
     *
     * Then with 2 indexes. We'll have the following scenarios:
     *
     *  * keep both in the same partition:
     *     * 4 states
     *  * keep in different partitions:
     *     * 2 states
     *     * 2 WFIT (one per each partition)
     */
    private static void testWfitAlgorithm(int reps) throws SQLException, IOException
    {
        log(reps == 1 ? "Real Test start...." : "Warm-up phase start....");

        for(int warmupIdx = 0; warmupIdx < reps; warmupIdx++){
            List<ProfiledQuery<DBIndex>> qinfos;
            CandidatePool<DBIndex> pool;
            Snapshot<DBIndex> snapshot;
            IndexPartitions<DBIndex> partitions;
            KarlsWorkFunctionAlgorithm<DBIndex> wfa;
            KarlsWorkFunctionAlgorithm.WfaTrace<DBIndex> trace;

            KarlsWFALog log;
            IndexBitSet[] wfitSchedule;
            IndexBitSet[] optSchedule;
            IndexBitSet[] minSched;
            String        workloadFile;
            String        bootstrapFile;
            double[]      overheads;
            double[]      minWfValues;
            int           maxNumIndexes;
            int           maxNumStates;
            int           queryCount;
            int           whatIfCount;
            boolean       exportWfit;
            boolean       exportOptWfit;
            boolean       exportOptWfitMins;

            maxNumIndexes     = env.getMaxNumIndexes();
            maxNumStates      = env.getMaxNumStates();
            bootstrapFile     = env.getScriptAtWorkloadsFolder("one_table/candidate_set_bootstrap_workload.sql");
            workloadFile      = env.getScriptAtWorkloadsFolder("one_table/workload.sql");
            pool              = getCandidates(connection, bootstrapFile);
            qinfos            = getOfflineProfiledQueries(connection, pool, workloadFile);
            queryCount        = qinfos.size();
            whatIfCount       = 0;
            overheads         = new double[queryCount];
            wfitSchedule      = new IndexBitSet[queryCount];
            snapshot          = pool.getSnapshot();
            partitions        = getIndexPartitions(snapshot, qinfos, maxNumIndexes, maxNumStates);
            exportWfit        = true; // in the WFIT mode this value is true.
            exportOptWfit     = false;
            exportOptWfitMins = false;
            wfa               = new KarlsWorkFunctionAlgorithm<DBIndex>(partitions, exportWfit);

            // only one subset for this small example
            assertThat(partitions.subsetCount(), is(1));

            for (int q = 0; q < wfitSchedule.length; q++) {

                ProfiledQuery<DBIndex> query = qinfos.get(q);

                if(warmupIdx == 0){
                    log("Query " + q);
                    log(query.toString());
                    log("---------------");
                }

                final StopWatch watch = new StopWatch();
                wfa.newTask(query);

                Iterable<DBIndex> rec = wfa.getRecommendation();

                wfitSchedule[q] = new IndexBitSet();

                for (DBIndex idx : rec) {
                    wfitSchedule[q].set(idx.internalId());
                }

                overheads[q] = watch.milliseconds();

                assertThat(query.getCandidateSnapshot().maxInternalId()+1, is(1));

                if(q < 5) {
                    assertThat(wfitSchedule[q].cardinality(), is(0));
                    assertThat(wfitSchedule[q].isEmpty(), is(true));
                    assertThat(query.getWhatIfCount()-whatIfCount, is(4));
                } else if(q == 5) {
                    assertThat(wfitSchedule[q].cardinality(), is(1));
                    assertThat(wfitSchedule[q].isEmpty(), is(false));
                    assertThat(query.getWhatIfCount()-whatIfCount, is(4));
                } else if(q == 6) {
                    assertThat(wfitSchedule[q].cardinality(), is(0));
                    assertThat(wfitSchedule[q].isEmpty(), is(true));
                    assertThat(query.getWhatIfCount()-whatIfCount, is(3));
                } else {
                    throw new SQLException("Workload should have 7 statements");
                }

                whatIfCount = query.getWhatIfCount();
            }

            if (exportWfit) {
                log = KarlsWFALog.generateFixed(qinfos, wfitSchedule, snapshot, partitions, overheads);

                for (int q = 0; q < wfitSchedule.length; q++) {
                }

                if(warmupIdx == 0){
                    log.dump();
                }
            }

            if (exportOptWfit) {
                trace       = wfa.getTrace();
                optSchedule = trace.optimalSchedule(partitions, qinfos.size(), qinfos);
                log         = KarlsWFALog.generateFixed(qinfos, optSchedule, snapshot, partitions, new double[queryCount]);
                if(warmupIdx == 0){
                    log.dump();
                }

                if (exportOptWfitMins) {
                    minWfValues = new double[qinfos.size()+1];

                    for (int q = 0; q <= qinfos.size(); q++) {
                        minSched       = trace.optimalSchedule(partitions, q, qinfos);
                        minWfValues[q] = wfa.getScheduleCost(snapshot, q, qinfos, partitions, minSched);

                        if(warmupIdx == 0){
                            log("Optimal cost " + q + " = " + minWfValues[q]);
                        }

                        for (int i = 0; i < q; i++) {
                            if(warmupIdx == 1){
                                log(str(minSched[i]));
                            }
                        }
                    }
                }
            }
        }

        log(reps == 1 ? "Real Test finished...." : "Warm-up phase finished....");
    }

    private static IndexPartitions<DBIndex> getIndexPartitions(
            Snapshot<DBIndex>            candidateSet,
            List<ProfiledQuery<DBIndex>> qinfos,
            int                          maxNumIndexes,
            int                          maxNumStates )
    {
        final StaticIndexSet<DBIndex> hotSet = KarlsHotsetSelector.chooseHotSet(
                candidateSet, new StaticIndexSet<DBIndex>(), new DynamicIndexSet<DBIndex>(),
                DBTuneInstances.newTempBenefitFunction(qinfos, candidateSet.maxInternalId()),
                maxNumIndexes, false
                );

        return KarlsInteractionSelector.choosePartitions(
                hotSet,
                new IndexPartitions<DBIndex>(hotSet),
                DBTuneInstances.newTempDoiFunction(qinfos, candidateSet),
                maxNumStates
                );
    }

    private static void log(String message) {
        Console.streaming().log(message);
    }

    private static CandidatePool<DBIndex> getCandidates(DatabaseConnection con, String workloadFilename)
        throws SQLException, IOException
    {
        CandidatePool<DBIndex> pool    = new CandidatePool<DBIndex>();
        File workloadFile              = new File(workloadFilename);
        Iterable<DBIndex> candidateSet;

        candidateSet = con.getIndexExtractor().recommendIndexes(workloadFile);

        for (DBIndex index : candidateSet) {
            pool.addIndex(index);
        }

        return pool;
    }

    private static List<ProfiledQuery<DBIndex>> getOfflineProfiledQueries(
            DatabaseConnection con, CandidatePool<DBIndex> pool, String workloadFilename)
        throws IOException
    {
        WorkloadProfiler<DBIndex> profiler = new WorkloadProfilerImpl<DBIndex>(con, pool, false);

        // get an IBG etc for each statement
        List<ProfiledQuery<DBIndex>> qinfos;
        List<String>                 lines;

        lines = Files.getLines(new File(workloadFilename));

        qinfos = new ArrayList<ProfiledQuery<DBIndex>>();

        for (String sql : lines) {
            qinfos.add(profiler.processQuery(DBUtilities.trimSqlStatement(sql)));
        }

        return qinfos;
    }
}
