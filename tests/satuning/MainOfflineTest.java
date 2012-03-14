package satuning;

//import java.io.File;
//import java.io.ObjectOutputStream;
//
//import satuning.db.DB2Index;
//import satuning.db.DB2IndexSet;
//import satuning.engine.CandidatePool;
//import satuning.engine.ProfiledQuery;
//import satuning.engine.CandidatePool.Snapshot;
//import satuning.engine.selection.IndexPartitions;
//import satuning.engine.selection.WorkFunctionAlgorithm;
//import satuning.engine.selection.WorkFunctionAlgorithm.TotalWorkValues;
//import satuning.offline.OfflineAnalysis;
//import satuning.util.BitSet;
//import satuning.util.Debug;
//import satuning.util.Files;

public class MainOfflineTest {

//	public static void main(String[] args) {
//		Configuration.subdirName = "mini";
//		Configuration.mode = "offline";
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
//			//CandidatePool pool = OfflineAnalysis.getCandidates(conn, workloadFile);
//			//ProfiledQuery[] qinfos = OfflineAnalysis.profileQueries(conn, workloadFile, pool);
//
//			CandidatePool pool = Main.readCandidatePool();
//			ProfiledQuery[] qinfos = Main.readProfiledQueries();
//			
//			IndexPartitions parts = OfflineAnalysis.getPartition(pool.getSnapshot(), qinfos, maxNumIndexes, maxNumStates);
//			TotalWorkValues[] wfValues = OfflineAnalysis.getTotalWorkValues(qinfos, pool, parts);
//			BitSet[] recs = WorkFunctionAlgorithm.optimalSchedule(parts, wfValues, qinfos);
//			
//			// create a few more structures for logging
//			Snapshot snapshot = pool.getSnapshot();
//			BitSet[] partitionBitSets = parts.bitSetArray();
//			DB2IndexSet indexSet = pool.getDB2IndexSet();
//			WFALog log = new WFALog();
//			
//			BitSet prevState = new BitSet();
//			for (int i = 0; i < qinfos.length; i++) {
//				double queryCost = qinfos[i].cost(recs[i]);
//				double transitionCost = WorkFunctionAlgorithm.transitionCost(snapshot, prevState, recs[i]);
//				log.add(qinfos[i], partitionBitSets, recs[i], queryCost, transitionCost, qinfos[i].whatifCount);
//				prevState = recs[i];
//			}
//			
//			File logFile = Configuration.logFile(Configuration.mode);
//			ObjectOutputStream out = new ObjectOutputStream(Files.initOutputFile(logFile));
//			try {
//				out.writeObject(log);
//				out.writeObject(indexSet);
//			} finally {
//				out.close();
//			}
//			
//			log.dump();
//			for (DB2Index index: indexSet) {
//				System.out.println(index.creationText());
//			}
//		} catch (Throwable e) {
//			Debug.logError("offline test threw an exception");
//			e.printStackTrace();
//			System.exit(1);
//		}
//		
//		System.exit(0);
//	}
}
