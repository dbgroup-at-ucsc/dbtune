package satuning.engine.profiling;

import java.sql.SQLException;

import satuning.db.DB2Index;
import satuning.db.DBConnection;
import satuning.db.ExplainInfo;
import satuning.engine.CandidatePool;
import satuning.engine.ProfiledQuery;
import satuning.engine.CandidatePool.Snapshot;
import satuning.ibg.IBGAnalyzer;
import satuning.ibg.IndexBenefitGraphConstructor;
import satuning.ibg.ThreadIBGAnalysis;
import satuning.ibg.ThreadIBGConstruction;
import satuning.ibg.log.InteractionLogger;
import satuning.util.*;

public class Profiler {
	private final DBConnection conn;
	private final CandidatePool candPool;
	private final ThreadIBGAnalysis ibgAnalysis;
	private final ThreadIBGConstruction ibgConstruction;
	private final boolean onlineCandidates;

	public Profiler(DBConnection conn0, CandidatePool candPool0, boolean onlineCandidates0) {
		conn = conn0;
		candPool = candPool0;
		onlineCandidates = onlineCandidates0;
		ibgAnalysis = new ThreadIBGAnalysis();
		Thread ibgAnalysisThread = new Thread(ibgAnalysis);
		ibgAnalysisThread.setName("IBG Analysis");
		ibgAnalysisThread.start();
		ibgConstruction = new ThreadIBGConstruction();
		Thread ibgContructionThread = new Thread(ibgConstruction);
		ibgContructionThread.setName("IBG Construction");
		ibgContructionThread.start();
	}
	
	public ProfiledQuery processQuery(String sql) {
		// get basic query info
		ExplainInfo explainInfo = null;
		try {
			explainInfo = conn.getExplainInfo(sql);
		} catch (SQLException e) {
			Debug.logError("SQLException caught while explaining command", e);
			throw new Error(e);
		}
		
		// generate new index candidates
		if (onlineCandidates) {
			try {
				Iterable<DB2Index> newIndexes = conn.recommendIndexes(sql);
				candPool.addIndexes(newIndexes);
			} catch (SQLException e) {
				Debug.logError("SQLException caught while recommending indexes", e);
			}
		}
		
		// get the current set of candidates
		Snapshot indexes = candPool.getSnapshot();
		
		// build the IBG
		try {
			conn.fixCandidates(indexes);
			InteractionLogger logger = new InteractionLogger(indexes);
			
			IndexBenefitGraphConstructor ibgCons = new IndexBenefitGraphConstructor(conn, sql, indexes);
			IBGAnalyzer ibgAnalyzer = new IBGAnalyzer(ibgCons);
			
			ibgConstruction.startConstruction(ibgCons);
			ibgAnalysis.startAnalysis(ibgAnalyzer, logger);
			ibgConstruction.waitUntilDone();
			ibgAnalysis.waitUntilDone();
			
			// pass the result to the tuner
			ProfiledQuery qinfo = new ProfiledQuery(sql, explainInfo, indexes, ibgCons.getIBG(), logger.getInteractionBank(), conn.whatifCount);
			return qinfo;
		} catch (SQLException e) {
			Debug.logError("SQLException caught while building ibg", e);
			throw new ToDoException(); // what to return here?
		}
	}
	
	public Snapshot processVote(DB2Index index, boolean isPositive) throws SQLException {
		if (isPositive)
			candPool.addIndex(index);
		return candPool.getSnapshot();
	}

	public Snapshot addCandidate(DB2Index index) throws SQLException {
		candPool.addIndex(index);
		return candPool.getSnapshot();
	}
}
