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

import edu.ucsc.dbtune.core.DBIndex;
import edu.ucsc.dbtune.core.DatabaseConnection;
import edu.ucsc.dbtune.core.DatabaseIndexExtractor;
import edu.ucsc.dbtune.core.DatabaseWhatIfOptimizer;
import edu.ucsc.dbtune.core.ExplainInfo;
import edu.ucsc.dbtune.ibg.CandidatePool;
import edu.ucsc.dbtune.ibg.IBGAnalyzer;
import edu.ucsc.dbtune.ibg.IndexBenefitGraph;
import edu.ucsc.dbtune.ibg.IndexBenefitGraphConstructor;
import edu.ucsc.dbtune.ibg.InteractionBank;
import edu.ucsc.dbtune.ibg.InteractionLogger;
import edu.ucsc.dbtune.ibg.ThreadIBGAnalysis;
import edu.ucsc.dbtune.ibg.ThreadIBGConstruction;
import edu.ucsc.dbtune.ibg.CandidatePool.Snapshot;
import edu.ucsc.dbtune.util.Debug;
import edu.ucsc.dbtune.util.Objects;
import edu.ucsc.dbtune.util.ToStringBuilder;

import java.sql.SQLException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @author huascar.sanchez@gmail.com (Huascar A. Sanchez)
 */
public class WorkloadProfilerImpl<I extends DBIndex> implements WorkloadProfiler<I> {
	private final DatabaseConnection    connection;
	private final CandidatePool<I>      candidatePool;
	private final ThreadIBGAnalysis     ibgAnalysis;
	private final ThreadIBGConstruction ibgConstruction;
	private final boolean               onlineCandidates;

    public WorkloadProfilerImpl(
            DatabaseConnection connection,
            CandidatePool<I> candidatePool,
            boolean onlineCandidates
    ){
        this(
                connection,
                candidatePool,
                new ThreadIBGAnalysis(),
                new ThreadIBGConstruction(),
                onlineCandidates
        );
    }

    WorkloadProfilerImpl(
            DatabaseConnection connection,
            CandidatePool<I> candidatePool,
            ThreadIBGAnalysis ibgAnalysis,
            ThreadIBGConstruction ibgConstruction,
            boolean onlineCandidates
    ){
        this.connection         = connection;
        this.candidatePool      = candidatePool;
        this.ibgAnalysis        = ibgAnalysis;
        this.ibgConstruction    = ibgConstruction;
        this.onlineCandidates   = onlineCandidates;
        runProcesses();
    }

    private void runProcesses(){
        final ExecutorService executor = Executors.newSingleThreadExecutor();
        executor.execute(ibgAnalysis);
        executor.execute(ibgConstruction);
        executor.shutdown();
    }

    
    @Override
    public Snapshot<I> addCandidate(I index) throws SQLException {
		candidatePool.addIndex(index);
		return candidatePool.getSnapshot();
    }

    @Override
    public ProfiledQuery<I> processQuery(String sql) {
        Debug.println("Profiling query: " + sql);

        final DatabaseIndexExtractor extractor = connection.getIndexExtractor();
		// generate new index candidates
		if (onlineCandidates) {
            acknowledgeOnlineCandidates(sql, extractor);
        }

        Snapshot<I>     indexes     = fixCandidatesInPool();
        ExplainInfo     explainInfo = obtainBasicQueryInformation(sql);
        return buildIndexBenefitGraph(sql, indexes, explainInfo);
    }

    ProfiledQuery<I> buildIndexBenefitGraph(String sql, Snapshot<I> indexes, ExplainInfo explainInfo) {
        // build the IBG
        final ProfiledQuery<I> qinfo;
        try {
            InteractionLogger logger = new InteractionLogger(indexes);

            IndexBenefitGraphConstructor<I> ibgCons = new IndexBenefitGraphConstructor<I>(connection, sql, indexes);
            IBGAnalyzer ibgAnalyzer = new IBGAnalyzer(ibgCons);

            ibgConstruction.startConstruction(ibgCons);
            ibgConstruction.waitUntilDone();
            ibgAnalysis.startAnalysis(ibgAnalyzer, logger);
            long nStart = System.nanoTime();
            ibgAnalysis.waitUntilDone();
            long nStop = System.nanoTime();
            System.out.println("Analysis: " + ((nStop - nStart) / 1000000000.0));

            System.out.println("IBG has " + ibgCons.nodeCount() + " nodes");
            final DatabaseWhatIfOptimizer whatIfOptimizer = connection.getWhatIfOptimizer();
            final int                        whatIfCount     = whatIfOptimizer.getWhatIfCount();
            final InteractionBank intBank         = logger.getInteractionBank();
            final IndexBenefitGraph graph           = ibgCons.getIBG();
            // pass the result to the tuner
            qinfo = new ProfiledQuery.Builder<I>(sql)
                    .explainInfo(explainInfo)
                    .snapshotOfCandidateSet(indexes)
                    .indexBenefitGraph(graph)
                    .interactionBank(intBank)
                    .whatIfCount(whatIfCount)
                    .indexBenefitGraphAnalysisTime(((nStop - nStart) / 1000000.0))
                .get();
        } catch (SQLException e) {
            Debug.logError("SQLException caught while building ibg", e);
            throw new IllegalStateException(); // what to return here?
        }
        return qinfo;
    }

    ExplainInfo obtainBasicQueryInformation(String sql) {
        final DatabaseWhatIfOptimizer optimizer = connection.getWhatIfOptimizer();
        ExplainInfo explainInfo;
        try {
            explainInfo = optimizer.explainInfo(sql);
        } catch (SQLException e) {
            Debug.logError("SQLException caught while explaining command", e);
            throw new Error(e);
        }
        return explainInfo;
    }

    Snapshot<I> fixCandidatesInPool() {
        final DatabaseWhatIfOptimizer optimizer = connection.getWhatIfOptimizer();
        // get the current set of candidates
        Snapshot<I> indexes = candidatePool.getSnapshot();
        try {
            optimizer.fixCandidates(indexes);
        } catch (SQLException e) {
            Debug.logError("SQLException caught while setting candidates", e);
            throw new Error(e);
        }
        return indexes;
    }

    @SuppressWarnings({"RedundantTypeArguments"})
    void acknowledgeOnlineCandidates(String sql, DatabaseIndexExtractor extractor) {
        try {
            final Iterable<DBIndex> recommendation = extractor.recommendIndexes(sql);
            if(recommendation != null){
                Iterable<I> newIndexes = Objects.<Iterable<I>>as(extractor.recommendIndexes(sql));
                candidatePool.addIndexes(newIndexes);
            }
        } catch (SQLException e) {
            Debug.logError("SQLException caught while recommending indexes", e);
            throw new IllegalStateException(e);
        }
    }

    @Override
    public Snapshot<I> processVote(I index, boolean isPositive) throws SQLException {
        return isPositive ? addCandidate(index) : candidatePool.getSnapshot();
    }

    @Override
    public String toString() {
        return new ToStringBuilder<WorkloadProfiler>(this)
               .add("connection", connection)
               .add("candidatePool", candidatePool)
               .add("ibgAnalysis", ibgAnalysis)
               .add("ibgConstruction", ibgConstruction)
               .add("onlineCandidates", onlineCandidates)
               .toString();
    }
}
