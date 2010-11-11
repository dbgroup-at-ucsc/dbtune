package edu.ucsc.satuning.engine;

import java.sql.SQLException;

import edu.ucsc.satuning.db.DBIndex;
import edu.ucsc.satuning.db.DBIndexSet;
import edu.ucsc.satuning.db.DatabaseConnection;
import edu.ucsc.satuning.util.DBUtilities;
import edu.ucsc.satuning.engine.CandidatePool.Snapshot;

public class TuningInterface<I extends DBIndex<I>> {
	DatabaseConnection<I> conn;
	TaskScheduler<I> scheduler;
	
	public TuningInterface(DatabaseConnection<I> conn0) {
		conn = conn0;
		scheduler = new TaskScheduler<I>(conn);
	}
	
	public AnalyzedQuery<I> analyzeSQL(String sql) throws SQLException {
		sql = DBUtilities.trimSqlStatement(sql);
		return scheduler.analyzeQuery(sql);
	}
	
	public ProfiledQuery<I> profileSQL(String sql) throws SQLException {
		sql = DBUtilities.trimSqlStatement(sql);
		return scheduler.profileQuery(sql);
	}
	
	public double executeProfiledQuery(ProfiledQuery<I> qinfo) throws SQLException {
		return scheduler.executeProfiledQuery(qinfo);
	}
	
	public void positiveVote(I index) {
		scheduler.positiveVote(index);
	}
	
	public void negativeVote(I index) {
		scheduler.negativeVote(index);
	}
	
	public double createIndex(I index) {
		scheduler.positiveVote(index);
		double transitionCost = scheduler.create(index);
		return transitionCost;
	}
	
	public double dropIndex(I index) {
		scheduler.negativeVote(index);
		double transitionCost = scheduler.drop(index);
		return transitionCost;
	}
	
	public DBIndexSet<I> getRecommendation() {
		DBIndexSet<I> indexSet = new DBIndexSet<I>();
		for (I index : scheduler.getRecommendation()) {
			indexSet.add(index);
		}
		return indexSet;
	}

	public Snapshot<I> addColdCandidate(I index) {
		return scheduler.addColdCandidate(index);
	}
}
