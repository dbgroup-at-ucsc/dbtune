package satuning;

//import java.io.File;
//import java.io.ObjectOutputStream;
//
//
//import satuning.engine.ProfiledQuery;
//import satuning.util.Debug;
//import satuning.util.Files;

public class MainTrimUpdatedTable {
	
	// disable this until we need it again
//	public static void main(String[] args) {
//		Configuration.subdirName = "mini";
//		
//		try {
//			ProfiledQuery[] qinfos = Main.readProfiledQueries();
//			for (ProfiledQuery qinfo : qinfos) {
//				qinfo.explainInfo.trimUpdatedTable();
//			}
//			
//			// write out queries
//			File queryFile = Configuration.profiledQueryFile();
//			ObjectOutputStream out = new ObjectOutputStream(Files.initOutputFile(queryFile));
//			try {
//				out.writeObject(qinfos);
//			} finally {
//				out.close();
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
