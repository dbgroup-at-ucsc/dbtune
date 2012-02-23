package interaction.cand;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import interaction.db.*;
import interaction.util.Files;

/*
 * Generates a recommendation according to the db2advis program
 * 
 * This is a typical invocation:
 * 
 *    db2advis -d karlsch -i workload.txt -m I -l -1 -k OFF -f -o recommendation.txt -a karlsch/****
 * 
 * Where
 *   -d karlsch = use database "karlsch"
 *   -i workload.txt = use queries in workload.txt
 *   -m I = recommend indexes
 *   -l -1 = no space budget
 *   -k OFF = no workload compression
 *   -f = drop previously existing simulated catalog tables (???)
 *   -o recommendation.xml = put recommendation into recommendation.xml
 *   -a karlsch/**** = username/password
 *   
 * The workload file must be an SQL file with semicolon-delimited queries 
 */
public class Advisor {
	private static final Pattern indexHeaderPattern = Pattern.compile("^-- index\\[\\d+\\],\\s+(.+)MB");
	private static final Pattern indexStatementPattern = Pattern.compile("^\\s*CREATE.+(IDX\\d*)\\\"");
	private static final Pattern startIndexesPattern = Pattern.compile("^-- LIST OF RECOMMENDED INDEXES");
	private static final Pattern endIndexesPattern = Pattern.compile("^-- RECOMMENDED EXISTING INDEXES"); 

	public static FileInfo createAdvisorFile(DBConnection conn, String advisorPath, int budget, File workloadFile) throws IOException, AdvisorException, SQLException {
		conn.qlib.clearAdviseIndex.execute(); // executes "DELETE FROM advise_index"
		
		String cmd = getCmd(conn, advisorPath, budget, workloadFile);
		Process prcs = Runtime.getRuntime().exec(cmd);
		
		FileInfo info;
		InputStream in = new BufferedInputStream(prcs.getInputStream());
		try {
			info = new FileInfo(in);
		} finally {
			in.close();
		}
		
		while (true) {
			try {
				prcs.waitFor();
				break;
			} catch (InterruptedException e) { }
		}
		int rc = prcs.exitValue();
		if (rc != 0)
			throw new AdvisorException("db2advis returned code "+rc);
		
		return info;
	}

	private static String getCmd(DBConnection conn, String advisorPath, int budget, File inFile) {
		String db = conn.dbName();
		String pw = conn.password();
		String user = conn.userName();
		
		return advisorPath
		       +" -d "+db
		       +" -a "+user+"/"+pw
		       +" -i "+inFile
		       +" -l "+budget
		       +" -m I -f -k OFF";
		
	}
	
	public static class FileInfo {
		List<String> indexNames;
		List<Double> indexMegabytes;
		
		private FileInfo(InputStream stream) throws IOException, AdvisorException {
			indexNames = new ArrayList<String>();
			indexMegabytes = new ArrayList<Double>();
			processFile(stream, indexNames, indexMegabytes);
		}
		
		public DB2IndexSet getCandidates(DBConnection conn) throws SQLException {
			DB2IndexSet candidateSet = new DB2IndexSet();
			int id = 1;
			for (String indexName : indexNames) {
				DB2Index idx = conn.qlib.readAdviseIndex.execute(indexName, id);
				System.out.println("Candidate Index " + id + ": " + indexMegabytes.get(id-1));
				System.out.println(idx.creationText());
				candidateSet.add(idx);
				++id;
			}
			// normalize the index candidates
			candidateSet.normalize();
			
			return candidateSet;
		}
		
		public int getMegabytes() {
			double total = 0;
			for (double v : indexMegabytes) 
				total += v;
			System.out.println("Total size = " + total);
			return (int) Math.round(total);
		}
		
		private static void processFile(InputStream stream, List<String> names, List<Double> sizes) throws IOException, AdvisorException { 
			List<String> lines = Files.getLines(stream); // splits the file into individual lines
			Iterator<String> iter = lines.iterator();
			Matcher headerMatcher = indexHeaderPattern.matcher("");
			Matcher startMatcher = startIndexesPattern.matcher("");
			Matcher endMatcher = endIndexesPattern.matcher("");
			Matcher createMatcher = indexStatementPattern.matcher("");
			while (iter.hasNext()) {
				String line = iter.next();
				startMatcher.reset(line);
				if (startMatcher.find())
					break;
			}
			
			while (iter.hasNext()) {
				String line = iter.next();
				endMatcher.reset(line);
				if (endMatcher.find())
					break;
				else {
					headerMatcher.reset(line);
					if (headerMatcher.find()) {
						createMatcher.reset(iter.next()); // advanced iterator! 
						if (!createMatcher.find())
							throw new AdvisorException("Unexpected advisor file format");
						
						names.add(createMatcher.group(1));
						sizes.add(Double.parseDouble(headerMatcher.group(1)));
						System.out.println(sizes.get(sizes.size()-1) + "\t" + names.get(names.size()-1)+line);
					}
				}
			}
		}
	}
}
