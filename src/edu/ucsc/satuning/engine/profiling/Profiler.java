package edu.ucsc.satuning.engine.profiling;

import java.sql.SQLException;

import edu.ucsc.satuning.db.DBIndex;
import edu.ucsc.satuning.db.DatabaseConnection;
import edu.ucsc.satuning.db.ExplainInfo;
import edu.ucsc.satuning.engine.CandidatePool;
import edu.ucsc.satuning.engine.ProfiledQuery;
import edu.ucsc.satuning.engine.CandidatePool.Snapshot;
import edu.ucsc.satuning.ibg.IBGAnalyzer;
import edu.ucsc.satuning.ibg.IndexBenefitGraphConstructor;
import edu.ucsc.satuning.ibg.ThreadIBGAnalysis;
import edu.ucsc.satuning.ibg.ThreadIBGConstruction;
import edu.ucsc.satuning.ibg.log.InteractionLogger;
import edu.ucsc.satuning.util.*;

public class Profiler<I extends DBIndex<I>> {
	private final DatabaseConnection<I> conn;
	private final CandidatePool<I> candPool;
	private final ThreadIBGAnalysis ibgAnalysis;
	private final ThreadIBGConstruction ibgConstruction;
	private final boolean onlineCandidates;

	public Profiler(DatabaseConnection<I> conn, CandidatePool<I> candPool, boolean onlineCandidates) {
		this.conn = conn;
		this.candPool = candPool;
		this.onlineCandidates = onlineCandidates;
		this.ibgAnalysis = new ThreadIBGAnalysis();
		Thread ibgAnalysisThread = new Thread(ibgAnalysis);
		ibgAnalysisThread.setName("IBG Analysis");
		ibgAnalysisThread.start();
		ibgConstruction = new ThreadIBGConstruction();
		Thread ibgContructionThread = new Thread(ibgConstruction);
		ibgContructionThread.setName("IBG Construction");
		ibgContructionThread.start();
	}
	
	public ProfiledQuery<I> processQuery(String sql) {
		Debug.println("Profiling query: " + sql);
		
		// generate new index candidates
		if (onlineCandidates) {
			try {
				Iterable<I> newIndexes = conn.getIndexExtractor().recommendIndexes(sql);
				candPool.addIndexes(newIndexes);
			} catch (SQLException e) {
				Debug.logError("SQLException caught while recommending indexes", e);
			}
		}
		
		// get the current set of candidates
		Snapshot<I> indexes = candPool.getSnapshot();
		try {
			conn.getIndexExtractor().fixCandidates(indexes);
		} catch (SQLException e) {
			Debug.logError("SQLException caught while setting candidates", e);
			throw new Error(e);
		}	
		
		// get basic query info
		ExplainInfo<I> explainInfo;
		try {
			explainInfo = conn.getIndexExtractor().explainInfo(sql);
		} catch (SQLException e) {
			Debug.logError("SQLException caught while explaining command", e);
			throw new Error(e);
		}
		
		// build the IBG
		try {
			InteractionLogger logger = new InteractionLogger(indexes);
			
			IndexBenefitGraphConstructor<I> ibgCons = new IndexBenefitGraphConstructor<I>(conn, sql, indexes);
			IBGAnalyzer ibgAnalyzer = new IBGAnalyzer(ibgCons);
			
			ibgConstruction.startConstruction(ibgCons);
			ibgConstruction.waitUntilDone();
			ibgAnalysis.startAnalysis(ibgAnalyzer, logger);
			long nStart = System.nanoTime();
			ibgAnalysis.waitUntilDone();
			long nStop = System.nanoTime();
			System.out.println("Analysis: " + ((nStop - nStart) / 1000000000.0));

			System.out.println("IBG has " + ibgCons.nodeCount() + " nodes");
			
			// pass the result to the tuner
			ProfiledQuery<I> qinfo = new ProfiledQuery<I>(sql, explainInfo, indexes, ibgCons.getIBG(), logger.getInteractionBank(), conn.getWhatIfOptimizer().getWhatIfCount(), ((nStop - nStart) / 1000000.0));
			return qinfo;
		} catch (SQLException e) {
			Debug.logError("SQLException caught while building ibg", e);
			throw new ToDoException(); // what to return here?
		}
	}
	
	public Snapshot<I> processVote(I index, boolean isPositive) throws SQLException {
		if (isPositive)
			candPool.addIndex(index);
		return candPool.getSnapshot();
	}

	public Snapshot<I> addCandidate(I index) throws SQLException {
		candPool.addIndex(index);
		return candPool.getSnapshot();
	}
}
