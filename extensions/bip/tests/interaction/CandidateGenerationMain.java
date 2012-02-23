package interaction;

import java.io.File;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.sql.SQLException;

import interaction.cand.*;
import static interaction.cand.Generation.Strategy.*;
import interaction.db.DB2Index;
import interaction.db.DB2IndexSet;
import interaction.db.DBConnection;
import interaction.util.Files;
import interaction.workload.SQLWorkload;

/*
 * Chooses index candidates for a workload
 * 
 * Multiple sets of candidates can be chosen according to different 
 * generation strategies (see Generation.java)
 */
public class CandidateGenerationMain {

	public static void main(String[] args) {
		// process arguments
		if (args.length > 0) {
			System.out.println("No arguments are accepted");
		}
		
		// do the steps
		try {
			runSteps();
		} catch (Throwable t) {	
			t.printStackTrace();
		} finally { 	
			// ensures that all threads exit
			System.exit(0);
		}
	}

	private static void runSteps() throws SQLException, IOException, AdvisorException, ClassNotFoundException {
		// Connect to database
		DBConnection conn = Main.openConnection();
		try {
			// create workload object and disk file
			File workloadFile = Configuration.workloadFile();
			SQLWorkload workload = Main.getWorkload();
			workload.createWorkloadFile(workloadFile);
			
			System.out.println(Files.readFile(workloadFile));
			
			// create the candidate files

			// UNION_OPTIMAL
			File optimalCandidateFile = Configuration.candidateFile(UNION_OPTIMAL);
			DB2IndexSet optimalSet = UnionOptimal.getCandidates(conn, workload);
			writeCandidates(optimalCandidateFile, optimalSet);
			
			System.out.println(Files.readObjectFromFile(optimalCandidateFile));
			System.out.println();
			
			
//			// FULL_BUDGET
			/*
			String db2Advis = Configuration.db2Advis;
			File fullCandidateFile = Configuration.candidateFile(FULL_BUDGET);
			Advisor.FileInfo fullInfo = Advisor.createAdvisorFile(conn, db2Advis, -1, workloadFile);
			DB2IndexSet fullSet = fullInfo.getCandidates(conn);
			writeCandidates(fullCandidateFile, fullSet);
			
			System.out.println(Files.readObjectFromFile(fullCandidateFile));
			System.out.println();
			*/

			// OPTIMAL_1C
			File onecolCandidateFile = Configuration.candidateFile(OPTIMAL_1C);
			//DB2IndexSet onecolSet = DB2Index.extractSingleColumns(fullSet);
			DB2IndexSet onecolSet = DB2Index.extractSingleColumns(optimalSet);
			writeCandidates(onecolCandidateFile, onecolSet);
			
			System.out.println(Files.readObjectFromFile(onecolCandidateFile));
			System.out.println();
			
//			
//			// HALF_BUDGET (relies on results from HALF_BUDGET
//			File halfCandidateFile = Configuration.candidateFile(HALF_BUDGET);
//			int fullBudget = fullInfo.getMegabytes();
//			Advisor.FileInfo halfInfo = Advisor.createAdvisorFile(conn, db2Advis, fullBudget/2, workloadFile);
//			DB2IndexSet halfSet = halfInfo.getCandidates(conn);
//			writeCandidates(halfCandidateFile, halfSet);
//
//			System.out.println(Files.readObjectFromFile(halfCandidateFile));
//			System.out.println();
			
		} finally {
			conn.commit();
			conn.close();
		}
	}
	
	private static void writeCandidates(File outputFile, DB2IndexSet indexes) throws IOException {
		ObjectOutputStream out = new ObjectOutputStream(Files.initOutputFile(outputFile));
		try {
			out.writeObject(indexes);
		} finally {
			out.close(); // closes underlying stream
		}
	}
}
