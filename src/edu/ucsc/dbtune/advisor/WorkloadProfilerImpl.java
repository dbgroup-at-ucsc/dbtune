package edu.ucsc.dbtune.advisor;

import edu.ucsc.dbtune.core.DBIndex;
import edu.ucsc.dbtune.core.DatabaseConnection;
import edu.ucsc.dbtune.core.ExplainInfo;
import edu.ucsc.dbtune.core.IBGWhatIfOptimizer;
import edu.ucsc.dbtune.core.IndexExtractor;
import edu.ucsc.dbtune.ibg.CandidatePool;
import edu.ucsc.dbtune.ibg.CandidatePool.Snapshot;
import edu.ucsc.dbtune.ibg.IBGAnalyzer;
import edu.ucsc.dbtune.ibg.IBGConstructionException;
import edu.ucsc.dbtune.ibg.IndexBenefitGraphConstructor;
import edu.ucsc.dbtune.ibg.InteractionLogger;
import edu.ucsc.dbtune.ibg.ThreadIBGAnalysis;
import edu.ucsc.dbtune.ibg.ThreadIBGConstruction;
import edu.ucsc.dbtune.spi.core.Console;
import edu.ucsc.dbtune.util.Objects;
import edu.ucsc.dbtune.util.Threads;

import java.sql.SQLException;
import java.util.concurrent.ExecutorService;

/**
 * Represents an workload profiler.
 */
public class WorkloadProfilerImpl<I extends DBIndex> implements WorkloadProfiler <I> {
	private final DatabaseConnection    connection;
	private final CandidatePool<I>      candidatePool;
	private final ThreadIBGAnalysis     ibgAnalysis;
	private final ThreadIBGConstruction ibgConstruction;
	private final boolean               onlineCandidates;

    // prints to screen details of the profiling process
    private final Console               console  = Console.streaming();
    private final ExecutorService       executor = Threads.explicitThreadPerCpuExecutor("Task: IBG Profiling", console);

    /**
     * Construct a {@code profiler} given a {@link CandidatePool pool of candidate indexes},
     * a {@link DatabaseConnection database connection}, and a flag which indicate whether we
     * are dealing with {@code online} candidates.
     * @param connection
     *      a live {@link DatabaseConnection connection}.
     * @param candidatePool
     *      a {@link CandidatePool pool of candidate indexes}.
     * @param onlineCandidates
     *      {@code true} if we are dealing with online candidates. {@code false} otherwise.
     */
	public WorkloadProfilerImpl(DatabaseConnection connection, CandidatePool<I> candidatePool, boolean onlineCandidates) {
        this(connection, new ThreadIBGAnalysis(), new ThreadIBGConstruction(), candidatePool, onlineCandidates);
	}

    /**
     * Construct a {@code profiler} given a {@link CandidatePool pool of candidate indexes},
     * a {@link DatabaseConnection database connection}, and a flag which indicate whether we
     * are dealing with {@code online} candidates.
     * @param connection
     *      a live {@link DatabaseConnection connection}.
     * @param analysis
     *      an {@link ThreadIBGAnalysis analysis} task.
     * @param construction
     *      a {@link ThreadIBGConstruction construction} task.
     * @param candidatePool
     *      a {@link CandidatePool pool of candidate indexes}.
     * @param onlineCandidates
     *      {@code true} if we are dealing with online candidates. {@code false} otherwise.
     */
    WorkloadProfilerImpl(DatabaseConnection connection,
                         ThreadIBGAnalysis analysis,
                         ThreadIBGConstruction construction,
                         CandidatePool<I> candidatePool,
                         boolean onlineCandidates
    ) {
		this.connection         = connection;
		this.candidatePool      = candidatePool;
		this.onlineCandidates   = onlineCandidates;
		this.ibgAnalysis        = analysis;
        this.ibgConstruction    = construction;
    }


    @Override
	public Snapshot<I> addCandidate(I index) throws SQLException {
		candidatePool.addIndex(index);
		return candidatePool.getSnapshot();
	}


    private void execute(ThreadIBGAnalysis analysis, ThreadIBGConstruction construction){
       executor.execute(analysis);
       executor.execute(construction);
    }

    public void shutdown(){
        executor.shutdown();
    }


    @Override
    public ProfiledQuery<I> processQuery(String sql){

		// generate new index candidates
		if (onlineCandidates) {
			try {
                final IndexExtractor    extractor           = connection.getIndexExtractor();
                final Iterable<DBIndex> recommendedIndexes  = extractor.recommendIndexes(sql);

                for(DBIndex each : recommendedIndexes){
                    candidatePool.addIndex(Objects.<I>as(each));
                }
			} catch (SQLException e) {
                console.error("SQLException caught while recommending indexes", e);
			}
		}

		// get the current set of candidates
		Snapshot<I> snapshot = candidatePool.getSnapshot();
        final IBGWhatIfOptimizer ibgWhatIfOptimizer = connection.getIBGWhatIfOptimizer();
		try {
			ibgWhatIfOptimizer.fixCandidates(snapshot);
		} catch (SQLException e) {
			console.error("SQLException caught while setting candidates", e);
			throw new Error(e);
		}

        ExplainInfo info;
		try {
			info = ibgWhatIfOptimizer.explain(sql);
            console.info("WorkloadProfilerImpl#processQuery(String) returned an ExplainInfo object=" + info) ;
		} catch (SQLException e) {
			console.error("SQLException caught while explaining command", e);
			throw new Error(e);
		}

		// build the IBG
		try {
			InteractionLogger               logger      = new InteractionLogger(snapshot);
			IndexBenefitGraphConstructor<I> ibgCons     = new IndexBenefitGraphConstructor<I>(connection, sql, snapshot);
			IBGAnalyzer                     ibgAnalyzer = new IBGAnalyzer(ibgCons);

			ibgConstruction.startConstruction(ibgCons);
			
            long nStart = System.nanoTime();
			ibgAnalysis.startAnalysis(ibgAnalyzer, logger);
			long nStop = System.nanoTime();

            // pass the result to the tuner
            return new ProfiledQuery.Builder<I>(sql)
                    .explainInfo(info)
                    .snapshotOfCandidateSet(snapshot)
                    .indexBenefitGraph(ibgCons.getIBG())
                    .interactionBank(logger.getInteractionBank())
                    .whatIfCount(ibgWhatIfOptimizer.getWhatIfCount())
                    .indexBenefitGraphAnalysisTime(((nStop - nStart) / 1000000.0))
                    .get();
		} catch (SQLException e) {
            final String msg = "SQLException caught while building ibg";
			console.error(msg, e);
			throw new IBGConstructionException(msg, e);
		}
    }

    @Override
	public Snapshot<I> processVote(I index, boolean isPositive) throws SQLException {
		return isPositive ? addCandidate(index) : candidatePool.getSnapshot();
	}

    /**
     * run the profiler, which involves two tasks: {@link ThreadIBGAnalysis analysis} and
     * {@link ThreadIBGConstruction construction} tasks.
     */
    public void runProfiler(){
        execute(ibgAnalysis, ibgConstruction);
    }
}