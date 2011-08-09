package edu.ucsc.dbtune.advisor;

import edu.ucsc.dbtune.core.DBTuneInstances;
import edu.ucsc.dbtune.core.DatabaseConnection;
import edu.ucsc.dbtune.core.ExplainInfo;
import edu.ucsc.dbtune.core.metadata.Index;
import edu.ucsc.dbtune.ibg.CandidatePool;
import edu.ucsc.dbtune.ibg.CandidatePool.Snapshot;
import edu.ucsc.dbtune.ibg.IndexBenefitGraph;
import edu.ucsc.dbtune.ibg.ThreadIBGAnalysis;
import edu.ucsc.dbtune.ibg.ThreadIBGConstruction;
import edu.ucsc.dbtune.spi.core.Console;
import org.hamcrest.CoreMatchers;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import static edu.ucsc.dbtune.core.DBTuneInstances.generateColumns;
import static edu.ucsc.dbtune.core.DBTuneInstances.generateDescVals;
import static edu.ucsc.dbtune.core.DBTuneInstances.newPGDatabaseConnectionManager;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

/**
 * @author huascar.sanchez@gmail.com (Huascar A. Sanchez)
 */
public class WorkloadProfilerTest {
    private WorkloadProfilerImpl nonConcurrentProfiler;
    private WorkloadProfilerImpl concurrentProfiler;

    @Before
    public void setUp() throws Exception {
        final DatabaseConnection  connection;

        connection            = newPGDatabaseConnectionManager().connect();
        final CandidatePool pool = candidatePool(new CandidatePool(), 5, 20);
        nonConcurrentProfiler = new WorkloadProfilerImpl(
                connection,
                pool,
                true
        );

        final ThreadIBGAnalysis analysis = new ThreadIBGAnalysis(){
            @Override
            public void run() {
                info("Running IBG Analysis Task.");
            }

            @Override
            public void waitUntilDone() {
                info("Waiting for a second.");
            }
        };

        final ThreadIBGConstruction construction = new ThreadIBGConstruction(){
            @Override
            public void run() {
                info("Running IBG Construction Task.");
            }

            @Override
            public void waitUntilDone() {
                info("Waiting for a second.");
            }
        };

        concurrentProfiler    = new WorkloadProfilerImpl(
                connection,
                analysis,
                construction,
                new CandidatePool(),
                true
        );
    }

    @Test
    public void testProcessVote() throws Exception {
        final Index index67 = newPGIndex(67, 65);
        final Index index68 = newPGIndex(68, 65);

        // Note: the internal id of any new index with positive vote added to the candidate pool
        // will be changed to the next maximum internal id stored in the candidate pool
        final Snapshot positiveSnapshot = nonConcurrentProfiler.processVote(index67, true);
        final Snapshot negativeSnapshot = nonConcurrentProfiler.processVote(index68, false);

        assertThat(positiveSnapshot.findIndexId(20), is(newPGIndex(20, 65)));
        assertThat(negativeSnapshot.findIndexId(21), CoreMatchers.<Object>nullValue());
    }


    @Ignore @Test
    public void testProcessQuery() throws Exception {
        final ProfiledQuery pq = concurrentProfiler.processQuery("SELECT * FROM R;");
        assertThat(pq, CoreMatchers.<Object>notNullValue());

        final ExplainInfo info                      = pq.getExplainInfo();
        final double      maintenanceCostOfIndex0   = info.getIndexMaintenanceCost(newPGIndex(0, 123456));
        final double      maintenanceCostOfIndex1   = info.getIndexMaintenanceCost(newPGIndex(1, 123456));
        final double      maintenanceCostOfIndex2   = info.getIndexMaintenanceCost(newPGIndex(2, 123456));

        assertThat(info.isQuery(), is(true));
        assertThat(info.isDML(), is(false));
        // since we are dealing with a query, then costs shouldn't be 0.0
        assertThat(Double.compare(maintenanceCostOfIndex0, 1.1) == 0.0, is(false));
        assertThat(Double.compare(maintenanceCostOfIndex1, 2.0) == 0.0, is(false));
        assertThat(Double.compare(maintenanceCostOfIndex2, 3.0) == 0.0, is(false));
        assertThat(pq.getWhatIfCount(), equalTo(2));

        final Snapshot snapshot = pq.getCandidateSnapshot();
        assertThat(snapshot.maxInternalId(), equalTo(2));

        final IndexBenefitGraph graph = pq.getIndexBenefitGraph();
        assertThat(Double.compare(graph.emptyCost(), 1.0) == 0.0, is(true));
    }

    @After
    public void tearDown() throws Exception {
        nonConcurrentProfiler = null;
        concurrentProfiler    = null;
    }

    private static CandidatePool candidatePool(CandidatePool pool, int interval, int howmany) throws Exception {
        int count = 0;
        for(int idx = 0; idx < howmany; idx++){
            if(idx == interval){
                ++count;
                interval = interval * 2;
            }
            pool.addIndex(newPGIndex(idx, 1234 + count));
        }
        return pool;
    }


    private static Index newPGIndex(int indexId, int schemaId) throws Exception {
       return DBTuneInstances.newPGIndex(indexId, schemaId, generateColumns(3), generateDescVals(3));
    }

    private static void info(String message){
        Console.streaming().info(message);
    }
}
