package satuning;

//import java.io.File;
//
//import satuning.db.DB2Index;
//import satuning.db.DB2IndexSet;
//import satuning.engine.CandidatePool;
//import satuning.engine.ProfiledQuery;
//import satuning.engine.CandidatePool.Snapshot;
//import satuning.engine.selection.IndexPartitions;
//import satuning.engine.selection.WfaTrace;
//import satuning.engine.selection.WorkFunctionAlgorithm;
//import satuning.engine.selection.WorkFunctionAlgorithm.TotalWorkValues;
//import satuning.offline.OfflineAnalysis;
//import satuning.util.BitSet;
//import satuning.util.Debug;
//import satuning.util.Files;

public class MainFixedTest {

//	public static void main(String[] args) {
//		Configuration.subdirName = "mini";
//		Configuration.mode = "fixed";
//		Configuration.onlineCandidates = false;
//		
//		int maxNumIndexes = Configuration.maxHotSetSize;
//		int maxNumStates = Configuration.maxNumStates;
//		
//		//DBConnection conn = Main.openConnectionOrExit();
//		
//		try {
//			// run the steps of offline analysis
//
//			CandidatePool pool = Main.readCandidatePool();
//			ProfiledQuery[] qinfos = Main.readProfiledQueries();
//			
//			Debug.println("read " + qinfos.length + " queries");
//			
//			IndexPartitions parts = OfflineAnalysis.getPartition(pool.getSnapshot(), qinfos, maxNumIndexes, maxNumStates);
//
//			WorkFunctionAlgorithm wfa = new WorkFunctionAlgorithm();
//			wfa.repartition(parts);
//			
//			//TotalWorkValues[] wfValues = OfflineAnalysis.getTotalWorkValues(qinfos, pool, parts);
//			//BitSet[] recs = WorkFunctionAlgorithm.optimalSchedule(parts, wfValues, qinfos);
//			
//			// create a few more structures for logging
//			Snapshot snapshot = pool.getSnapshot();
//			BitSet[] partitionBitSets = parts.bitSetArray();
//			DB2IndexSet indexSet = pool.getDB2IndexSet();
//			WFALog fixedLog = new WFALog();
//			
//			BitSet lastRec = new BitSet();
//			int q = 1;
//			for (ProfiledQuery query : qinfos) {
//				Debug.println("issuing query: " + query.sql);
//				
//				// analyze the query and get the recommendation
//				wfa.newTask(query);
//				BitSet newRec = new BitSet();
//				for (DB2Index idx : wfa.getRecommendation()) {
//					newRec.set(idx.internalId());
//				}
//				double transitionCost = WorkFunctionAlgorithm.transitionCost(snapshot, lastRec, newRec);
//				double queryCost = query.cost(newRec);				
//				
//				// do bookkeeping
//				lastRec = newRec;
//				fixedLog.add(query, partitionBitSets, lastRec, queryCost, transitionCost, query.whatifCount);
//				++q;
//			}
//			
//			WfaTrace trace = wfa.getTrace();
//			TotalWorkValues[] wfValues = trace.getTotalWorkValues();
//			double[] minWfValues = trace.getMinWfValues();
//			
//			File fixedLogFile = Configuration.logFile("fixed");
//			Main.writeLog(fixedLogFile, fixedLog, snapshot);
//
//			Debug.println();
//			Debug.println("wrote log to " + fixedLogFile);
//			fixedLog.dump();
//			for (DB2Index index: indexSet) {
//				System.out.println(index.creationText());
//			}
//			
//			// write OPT results
//			BitSet[] recs = WorkFunctionAlgorithm.optimalSchedule(parts, wfValues, qinfos);
//			q = 0;
//			WFALog offlineLog = new WFALog();
//			lastRec = new BitSet();
//			for (ProfiledQuery qinfo : qinfos) {
//				Debug.println("OFFLINE state "+(q+1)+": " + recs[q]);
//				double queryCost = qinfo.cost(recs[q]);
//				double transitionCost = WorkFunctionAlgorithm.transitionCost(snapshot, lastRec, recs[q]);
//				offlineLog.add(qinfo, partitionBitSets, recs[q], queryCost, transitionCost, qinfo.whatifCount);
//				lastRec = recs[q];
//				++q;
//			}
//			
//			File offlineLogFile = Configuration.logFile("opt");
//			Main.writeLog(offlineLogFile, offlineLog, snapshot);
//			
//			Debug.println();
//			Debug.println("wrote log to " + offlineLogFile);
//			//log.dump();
//			
//			// write min wf values
//			File wfFile = Configuration.minWfFile();
//			Files.writeObjectToFile(wfFile, minWfValues);
//			
//		} catch (Throwable e) {
//			Debug.logError("offline test threw an exception");
//			e.printStackTrace();
//			System.exit(1);
//		}
//		
//		System.exit(0);
//	}
}
