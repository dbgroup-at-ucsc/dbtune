/*
 * ****************************************************************************
 *   Copyright 2010 University of California Santa Cruz                       *
 *                                                                            *
 *   Licensed under the Apache License, Version 2.0 (the "License");          *
 *   you may not use this file except in compliance with the License.         *
 *   You may obtain a copy of the License at                                  *
 *                                                                            *
 *       http://www.apache.org/licenses/LICENSE-2.0                           *
 *                                                                            *
 *   Unless required by applicable law or agreed to in writing, software      *
 *   distributed under the License is distributed on an "AS IS" BASIS,        *
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. *
 *   See the License for the specific language governing permissions and      *
 *   limitations under the License.                                           *
 *  ****************************************************************************
 */

package edu.ucsc.dbtune.core;

import edu.ucsc.dbtune.core.metadata.DB2Index;
import edu.ucsc.dbtune.core.metadata.DB2IndexMetadata;
import edu.ucsc.dbtune.spi.core.Commands;
import edu.ucsc.dbtune.util.Debug;
import edu.ucsc.dbtune.util.Files;
import edu.ucsc.dbtune.util.Objects;

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

import static edu.ucsc.dbtune.core.metadata.DB2Commands.clearAdviseIndex;
import static edu.ucsc.dbtune.core.metadata.DB2Commands.readAdviseOnOneIndex;
import static edu.ucsc.dbtune.core.metadata.DB2ReifiedTypes.DB2IndexSet;

/**
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
 * We don't create an output xml file ... just read the db2advis output directly 
 */
//todo(Huascar) make it an stateful object rather than leaving it like a util class.
public class Advisor {
	private static final Pattern INDEX_HEADER_PATTERN       = Pattern.compile("^-- index\\[\\d+\\],\\s+(.+)MB");
	private static final Pattern INDEX_STATEMENT_PATTERN    = Pattern.compile("^\\s*CREATE.+(IDX\\d*)\\\"");
	private static final Pattern START_INDEXES_PATTERN      = Pattern.compile("^-- LIST OF RECOMMENDED INDEXES");
	private static final Pattern END_INDEXES_PATTERN        = Pattern.compile("^-- RECOMMENDED EXISTING INDEXES");

	public static FileInfo createAdvisorFile(DatabaseConnection<DB2Index> conn, String advisorPath, int budget, File workloadFile) throws IOException, AdvisorException, SQLException {
		Commands.submit(
                // executes "DELETE FROM advise_index"
                clearAdviseIndex(), conn
        );

		final String cmd = getCmd(conn, advisorPath, budget, workloadFile, false);
		final String cleanCmd = getCmd(conn, advisorPath, budget, workloadFile, true);

        Debug.println("Running db2advis on " + workloadFile);
		Debug.println("command = " + cleanCmd);

        Process prcs = Runtime.getRuntime().exec(cmd);
		
		FileInfo info;
		InputStream in   = new BufferedInputStream(prcs.getInputStream());
		InputStream err  = new BufferedInputStream(prcs.getErrorStream());
		String errString = "";
		try {
			info        = new FileInfo(in);
			errString   = Files.readStream(err);
		} finally {
			in.close();
			err.close();
		}
		
		while (true) {
			try {
				prcs.waitFor();
				break;
			} catch (InterruptedException e) {
                Debug.println(e);
            }
		}
		int rc = prcs.exitValue();
		if (rc != 0){
            throw new AdvisorException("db2advis returned code "+rc+"\n"+errString);
        }
		
		return info;
	}

	private static String getCmd(DatabaseConnection<DB2Index> conn, String advisorPath, int budget, File inFile, boolean clean) {
        final JdbcDatabaseConnectionManager<DB2Index> manager = Objects.as(conn.getConnectionManager());
		final String db   = manager.getDatabaseName();
		final String pw   = manager.getPassword();
		final String user = manager.getUsername();
		
		return advisorPath
		       +" -d "+db
		       +" -a "+user+"/"+(clean?"****":pw)
		       +" -i "+inFile
		       +" -l "+budget
		       +" -m I -f -k OFF";
		
	}
	
	public static class FileInfo {
		List<IndexInfo> indexList;
		
		@SuppressWarnings("unused")
		private String output;
		
		private FileInfo(InputStream stream) throws IOException, AdvisorException {
			indexList = new ArrayList<IndexInfo>();
			output    = processFile(stream, indexList);
		}
		
		public DB2IndexSet getCandidates(DatabaseConnection<DB2Index> conn) throws SQLException {
			final List<DB2IndexMetadata> metaList = new ArrayList<DB2IndexMetadata>();
			int id = 1;
			for (IndexInfo info : indexList) {
                final DB2IndexMetadata meta = Commands.supplyValue(
                        readAdviseOnOneIndex(),
                        conn,
                        info.name,
                        id,
                        info.megabytes
                );
				metaList.add(meta);
				++id;
			}
			
			// post-process to get creation costs
			DB2IndexSet candidateSet = new DB2IndexSet();
			for (DB2IndexMetadata meta : metaList) {
				final double    creationCost = meta.creationCost(conn.getIndexExtractor(), conn.getWhatIfOptimizer());
				final DB2Index index        = new DB2Index(meta, creationCost);
				candidateSet.add(index);
			}
			
			// normalize the index candidates
			candidateSet.normalize();
			
			for (DB2Index index : candidateSet) {
				System.out.println("Candidate Index " + index.internalId() + ": " + index.megabytes());
				System.out.println(index.creationText());
			}
			
			return candidateSet;
		}
		
		public int getMegabytes() {
			double total = 0;
			for (IndexInfo info : indexList) 
				total += info.megabytes;
			System.out.println("Total size = " + total);
			return (int) Math.round(total);
		}
		
		private static String processFile(InputStream stream, List<IndexInfo> indexList) throws IOException, AdvisorException { 
			List<String> lines = Files.getLines(stream); // splits the file into individual lines
			Iterator<String> iter   = lines.iterator();
			Matcher headerMatcher   = INDEX_HEADER_PATTERN.matcher("");
			Matcher startMatcher    = START_INDEXES_PATTERN.matcher("");
			Matcher endMatcher      = END_INDEXES_PATTERN.matcher("");
			Matcher createMatcher   = INDEX_STATEMENT_PATTERN.matcher("");
			while (iter.hasNext()) {
				String line = iter.next();
				startMatcher.reset(line);
				if (startMatcher.find())
					break;
			}
			
			String str = "";
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
						
						String indexName = createMatcher.group(1);
						double indexMegabytes = Double.parseDouble(headerMatcher.group(1));
						indexList.add(new IndexInfo(indexName, indexMegabytes));
						System.out.println(indexMegabytes + "\t" + indexName + "\t" + line);
					}
				}
				str += line + "\n";
			}
			return str;
		}
	}
	
	private static class IndexInfo {
		String name;
		double megabytes;
		
		IndexInfo(String n, double m) {
			name = n;
			megabytes = m;
		}
	}
}
