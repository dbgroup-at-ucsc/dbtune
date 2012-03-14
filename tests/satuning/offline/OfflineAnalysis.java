package satuning.offline;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.util.List;

import satuning.Configuration;
import satuning.advisor.Advisor;
import satuning.advisor.AdvisorException;
import satuning.advisor.Advisor.FileInfo;
import satuning.db.DB2Index;
import satuning.db.DB2IndexSet;
import satuning.db.DBConnection;
import satuning.engine.CandidatePool;
import satuning.engine.ProfiledQuery;
import satuning.engine.CandidatePool.Snapshot;
import satuning.engine.profiling.Profiler;
import satuning.engine.selection.BenefitFunction;
import satuning.engine.selection.DoiFunction;
import satuning.engine.selection.DynamicIndexSet;
import satuning.engine.selection.HotSetSelector;
import satuning.engine.selection.IndexPartitions;
import satuning.engine.selection.InteractionSelector;
import satuning.engine.selection.StaticIndexSet;
//import satuning.engine.selection.WorkFunctionAlgorithm;
//import satuning.engine.selection.WorkFunctionAlgorithm.TotalWorkValues;
import satuning.ibg.log.InteractionBank;
//import satuning.util.BitSet;
import satuning.util.Files;

public class OfflineAnalysis {
//	public static BitSet[] runOPT(DBConnection conn, ProfiledQuery[] qinfos, CandidatePool pool, IndexPartitions parts) throws SQLException {
//		int queryCount = qinfos.length;
//		
//		WorkFunctionAlgorithm wfa = new WorkFunctionAlgorithm();
//		TotalWorkValues[] wfValues = new TotalWorkValues[queryCount+1];
//		
//		// initialize wfa with partitions
//		wfa.repartition(parts);
//		wfValues[0] = wfa.getTotalWorkValues();
//		
//		// run the queries through wfa
//		int q = 1;
//		for (ProfiledQuery qinfo : qinfos) {
//			wfa.newTask(qinfo);
//			wfValues[q] = wfa.getTotalWorkValues();
//			++q;
//		}
//		
//		// retrieve the optimal schedule
//		BitSet[] recs = WorkFunctionAlgorithm.optimalSchedule(parts, wfValues, qinfos);
//		
//		return recs;
//	}
//	
//	public static TotalWorkValues[] getTotalWorkValues(ProfiledQuery[] qinfos, CandidatePool pool, IndexPartitions parts) throws SQLException {
//		int queryCount = qinfos.length;
//		
//		WorkFunctionAlgorithm wfa = new WorkFunctionAlgorithm();
//		TotalWorkValues[] wfValues = new TotalWorkValues[queryCount+1];
//		
//		// initialize wfa with partitions
//		wfa.repartition(parts);
//		wfValues[0] = wfa.getTotalWorkValues();
//		
//		// run the queries through wfa
//		int q = 1;
//		for (ProfiledQuery qinfo : qinfos) {
//			wfa.newTask(qinfo);
//			wfValues[q] = wfa.getTotalWorkValues();
//			++q;
//		}
//		
//		return wfValues;
//	}
	
	public static CandidatePool getCandidates(DBConnection conn, File workloadFile) throws SQLException {
		// run the advisor to get the initial candidates
		// Give a budget of -1 to mean "no budget"
		DB2IndexSet candidateSet = null;
		try {
			FileInfo advisorFile = Advisor.createAdvisorFile(conn, Configuration.db2Advis, -1, workloadFile);
			candidateSet = advisorFile.getCandidates(conn);
		} catch (IOException e) {
			e.printStackTrace();
			System.exit(1);
		} catch (AdvisorException e) {
			e.printStackTrace();
			System.exit(1);
		}
		
		// add each candidate
		CandidatePool pool = new CandidatePool();
		for (DB2Index index : candidateSet) {
			pool.addIndex(index);
		}
		
		return pool;
	}
	
	public static ProfiledQuery[] profileQueries(DBConnection conn, File workloadFile, CandidatePool pool) throws SQLException {
		Profiler profiler = new Profiler(conn, pool, false);
		
		// get an IBG etc for each statement
		List<String> lines = null;
		ProfiledQuery[] qinfos = null;
		try {
			lines = Files.getLines(workloadFile);
		} catch (IOException e) {
			e.printStackTrace();
			System.exit(1);
		}
		
		qinfos = new ProfiledQuery[lines.size()];
		{
			int i = 0;
			for (String sql : lines) {
				qinfos[i++] = profiler.processQuery(DBConnection.trimSqlStatement(sql));
			}
		}
		
		return qinfos;
	}
	
	public static IndexPartitions getPartition(Snapshot candidateSet, ProfiledQuery[] qinfos, 
			                             int maxNumIndexes, int maxNumStates) throws SQLException {		
		// get the hot set
		StaticIndexSet hotSet = getHotSet(candidateSet, qinfos, maxNumIndexes);
		
		// partition the hot set
		DoiFunction doiFunc = new TempDoiFunction(qinfos, candidateSet);
		IndexPartitions parts = InteractionSelector.choosePartitions(hotSet,
																	 new IndexPartitions(hotSet),
																	 doiFunc,
																	 maxNumStates);
		
		return parts;
	}
	
	public static StaticIndexSet getHotSet(Snapshot candidateSet, ProfiledQuery[] qinfos, 
			                             int maxNumIndexes) {
		// get the hot set
		BenefitFunction benefitFunc = new TempBenefitFunction(qinfos, candidateSet.maxInternalId());
		StaticIndexSet hotSet = HotSetSelector.chooseHotSet(candidateSet, 
														    new StaticIndexSet(),
														    new DynamicIndexSet(),
														    benefitFunc,
														    maxNumIndexes);
		return hotSet;
	}

	private static class TempBenefitFunction implements BenefitFunction {
		private double[] benefits;
		
		TempBenefitFunction(ProfiledQuery[] qinfos, int maxInternalId) {
			benefits = new double[maxInternalId+1];
			for (ProfiledQuery qinfo : qinfos) {
				for (DB2Index index : qinfo.candidateSet) {
					int id = index.internalId();
					benefits[id] += qinfo.bank.bestBenefit(id);
				}
			}
		}
		
		public double benefit(DB2Index a) {
			return benefits[a.internalId()];
		}
	}

	private static class TempDoiFunction implements DoiFunction {
		private InteractionBank bank;
		TempDoiFunction(ProfiledQuery[] qinfos, Snapshot candidateSet) {
			bank = new InteractionBank(candidateSet);
			for (DB2Index a : candidateSet) {
				int id_a = a.internalId();
				for (DB2Index b : candidateSet) {
					int id_b = b.internalId();
					if (id_a < id_b) {
						double doi = 0;
						for (ProfiledQuery qinfo : qinfos) {
							doi += qinfo.bank.interactionLevel(a.internalId(), b.internalId());
						}
						bank.assignInteraction(a.internalId(), b.internalId(), doi);
					}
				}
			}
		}
		
		public double doi(DB2Index a, DB2Index b) {
			return bank.interactionLevel(a.internalId(), b.internalId());
		}
	}
}
