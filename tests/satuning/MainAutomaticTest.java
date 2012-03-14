package satuning;

//import java.io.File;
//import java.io.ObjectOutputStream;
//
//import satuning.db.DB2Index;
//import satuning.db.DB2IndexSet;
//import satuning.db.DBConnection;
//import satuning.engine.AnalyzedQuery;
//import satuning.engine.ProfiledQuery;
//import satuning.engine.CandidatePool.Snapshot;
//import satuning.engine.selection.Selector;
//import satuning.util.Debug;
//import satuning.util.Files;


public class MainAutomaticTest {
//
//	public static void main(String[] args) {
//		Configuration.subdirName = "automatic";
//		Configuration.mode = "online";
//		Configuration.onlineCandidates = true;
//		
//		DBConnection conn = Main.openConnectionOrExit();
//		Selector selector = new Selector();
//		
//		try {
//
//			ProfiledQuery[] qinfos = Main.readProfiledQueries();
//			
//			WFALog log = new WFALog();
//			DB2IndexSet lastRec = new DB2IndexSet();
//			Snapshot lastCandSet = null;
//			for (ProfiledQuery query : qinfos) {
//				Debug.println("issuing query: " + query);
//				
//				// analyze the query and get the recommendation
//				AnalyzedQuery qinfo = selector.analyzeQuery(query);
//				DB2IndexSet newRec = new DB2IndexSet();
//				for (DB2Index idx : selector.getRecommendation()) 
//					newRec.add(idx);
//				
//				// implement the recommendation
//				// record the cost
//				// no need to make votes because we're instantly accepting the recommendation
//				double transitionCost = 0;
//				for (DB2Index index : lastRec) {
//					if (!newRec.bitSet().get(index.internalId()))
//						transitionCost += selector.drop(index);
//				}
//				for (DB2Index index : newRec) {
//					if (!lastRec.bitSet().get(index.internalId()))
//						transitionCost += selector.create(index);
//				}
//				
//				// execute the query under the new configuration
//				double queryCost = selector.currentCost(qinfo.profileInfo);
//				
//				// do bookkeeping
//				lastRec = newRec;
//				lastCandSet = qinfo.profileInfo.candidateSet;
//				log.add(qinfo, lastRec.bitSet(), queryCost, transitionCost, conn.whatifCount);
//			}
//
//			File logFile = Configuration.logFile(Configuration.mode);
//			Main.writeLog(logFile, log, lastCandSet);
//			ObjectOutputStream out = new ObjectOutputStream(Files.initOutputFile(logFile));
//			try {
//				out.writeObject(log);
//				out.writeObject(lastCandSet);
//			} finally {
//				out.close();
//			}
//			
//			log.dump();
//			for (DB2Index index: lastCandSet) {
//				System.out.println(index.creationText());
//			}
//		} catch (Throwable e) {
//			Debug.logError("automatic test threw an exception");
//			e.printStackTrace();
//			Main.tryToCloseConnection(conn);
//			System.exit(1);
//		}
//		
//		Main.tryToCloseConnection(conn);
//		System.exit(0);
//	}
}
