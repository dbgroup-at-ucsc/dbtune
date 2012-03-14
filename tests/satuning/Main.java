package satuning;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.PrintStream;
import java.sql.SQLException;
import java.util.List;

import satuning.admin.BadAdmin;
import satuning.admin.GoodAdmin;
import satuning.admin.SlowAdmin;
import satuning.admin.WorkloadRunner;
import satuning.db.DB2Index;
import satuning.db.DBConnection;
import satuning.engine.AnalyzedQuery;
import satuning.engine.CandidatePool;
import satuning.engine.ProfiledQuery;
import satuning.engine.CandidatePool.Snapshot;
import satuning.engine.bc.BcTuner;
import satuning.engine.profiling.Profiler;
import satuning.engine.selection.IndexPartitions;
import satuning.engine.selection.Selector;
import satuning.engine.selection.StaticIndexSet;
import satuning.engine.selection.WfaTrace;
import satuning.engine.selection.WorkFunctionAlgorithm;
import satuning.offline.OfflineAnalysis;
import satuning.util.BitSet;
import satuning.util.Debug;
import satuning.util.Files;

public class Main {	
	public static void main(String[] args) {
		try {
            //Configuration.processArgs(args);
			if (Configuration.logging) {
				runLogging(Configuration.mode);
			}
			else { 
				Configuration.mode.run();
			}
		} catch (Throwable t) {
			System.err.print("Uncaught: ");
			System.err.println(t);
			t.printStackTrace();
			System.exit(1);
		}
		System.exit(0);
	}

	protected static DBConnection openConnectionOrExit() {
		DBConnection conn = null;
		try {
			conn = openConnection();
		} catch (Exception e) {
			Debug.logError("Failed to connect to database");
			e.printStackTrace();
			System.exit(1);
		}
		return conn;
	}

	public static void tryToCloseConnection(DBConnection conn) {
		try {
			conn.close();
		} catch (SQLException e) {
			Debug.logError("Failed to close database");
			e.printStackTrace();
		}
	}

	protected static DBConnection openConnection() throws SQLException, IOException {
		DBConnection conn = new DBConnection();
		conn.open(Configuration.dbName,
				  Configuration.url,
				  Configuration.userName,
				  null, // don't provide password
				  Configuration.driverClass);
		
		return conn;
	}

	public static List<String> getLinesOrExit(File file) {
		try {
			return Files.getLines(file);
		}
		catch (IOException e) {
			Debug.logError("Failed to read file");
			e.printStackTrace();
			System.exit(1);
			return null; // for compiler
		}
	}
	
	public static CandidatePool readCandidatePool() throws IOException, ClassNotFoundException {
		File file = Configuration.mode.candidatePoolFile();
		ObjectInputStream in = new ObjectInputStream(new FileInputStream(file));
		try {
			return (CandidatePool) in.readObject();
		} finally {
			in.close(); // closes underlying stream
		}
	}

	public static ProfiledQuery[] readProfiledQueries() throws IOException, ClassNotFoundException {
		File file = Configuration.mode.profiledQueryFile();
		ObjectInputStream in = new ObjectInputStream(new FileInputStream(file));
		try {
			return (ProfiledQuery[]) in.readObject();
		} finally {
			in.close(); // closes underlying stream
		}
	}
	
	public static void writeLog(File file, WFALog log, Snapshot snapshot) throws IOException {
		ObjectOutputStream out = new ObjectOutputStream(Files.initOutputFile(file));
		try {
			out.writeObject(log);
			out.writeObject(snapshot);
		} finally {
			out.close();
		}
	}
	
	public static WFALog readLog(File logFile) throws Exception {
		ObjectInputStream in = new ObjectInputStream(new FileInputStream(logFile));
		try {
			WFALog log = (WFALog) in.readObject();
			return log;
		} finally {
			in.close(); // closes underlying stream
		}
	}
	
	public static void runBC() throws Exception {	
		int maxNumIndexes = Configuration.maxHotSetSize;
		DBConnection conn = Main.openConnectionOrExit();
		
		CandidatePool pool = Main.readCandidatePool();
		ProfiledQuery[] qinfos = Main.readProfiledQueries();
		int queryCount = qinfos.length;
		
		Debug.println("read " + queryCount + " queries");
		
		Snapshot snapshot = pool.getSnapshot();
		StaticIndexSet hotSet = OfflineAnalysis.getHotSet(snapshot, qinfos, maxNumIndexes);
		IndexPartitions parts = new IndexPartitions(hotSet);
		BcTuner bc = new BcTuner(conn, snapshot, hotSet);

		BitSet[] recs = new BitSet[queryCount];
		for (int q = 0; q < queryCount; q++) {
			ProfiledQuery qinfo = qinfos[q];
			bc.processQuery(qinfo);
			recs[q] = bc.getRecommendation();
		}
		
		WFALog log = WFALog.generateFixed(qinfos, recs, snapshot, parts);
		
		File logFile = Configuration.mode.logFile();
		writeLog(logFile, log, pool.getSnapshot());
		processLog(logFile);
		
		log.dump();
	}
	

	public static void runWFIT(WorkloadRunner runner, boolean exportWfit, boolean exportOpt) throws Exception {
		int maxNumIndexes = Configuration.maxHotSetSize;
		int maxNumStates = Configuration.maxNumStates;
		
		CandidatePool pool = Main.readCandidatePool();
		ProfiledQuery[] qinfos = Main.readProfiledQueries();
		int queryCount = qinfos.length;
		Debug.println("read " + queryCount + " queries");
	
		Snapshot snapshot = pool.getSnapshot();
		IndexPartitions parts = OfflineAnalysis.getPartition(snapshot, qinfos, maxNumIndexes, maxNumStates);

		boolean wfaKeepHistoryOption = exportOpt;
		WorkFunctionAlgorithm wfa = new WorkFunctionAlgorithm(parts, wfaKeepHistoryOption);
			
		// run workload
		BitSet[] wfitSchedule = runner.getRecs(qinfos, wfa);
		
		if (exportWfit) {
			WFALog log = WFALog.generateFixed(qinfos, wfitSchedule, snapshot, parts);
			File logFile = Mode.WFIT.logFile();
			writeLog(logFile, log, snapshot);
			processLog(logFile);
			Debug.println();
			Debug.println("wrote log to " + logFile);
			log.dump();
			for (DB2Index index: snapshot) {
				System.out.println(index.creationText());
			}
		}
		
		if (exportOpt) {
			WfaTrace trace = wfa.getTrace();

			BitSet[] optSchedule = trace.optimalSchedule(parts, qinfos.length, qinfos);
			WFALog log = WFALog.generateFixed(qinfos, optSchedule, snapshot, parts);
			File logFile = Mode.OPT.logFile();
			writeLog(logFile, log, snapshot);
			processLog(logFile);
			Debug.println();
			Debug.println("wrote log to " + logFile);
			log.dump();
			
			// write min wf values
			double[] minWfValues = new double[qinfos.length+1];
			for (int q = 0; q <= qinfos.length; q++) {
				BitSet[] minSched = trace.optimalSchedule(parts, q, qinfos); 
				minWfValues[q] = WorkFunctionAlgorithm.getScheduleCost(snapshot, q, qinfos, minSched);
				Debug.println("Optimal cost " + q + " = " + minWfValues[q]);
			}
			File wfFile = Configuration.minWfFile();
			Files.writeObjectToFile(wfFile, minWfValues);
			processWfFile(wfFile);
		}
	}
	
	public static void runOnlineProfiling() throws Exception {
		File workloadFile = Configuration.workloadFile();
		
		DBConnection conn = Main.openConnectionOrExit();
		CandidatePool pool = new CandidatePool();
		Profiler profiler = new Profiler(conn, pool, true);
		
		try {
			List<String> sqlList = Files.getLines(workloadFile);
			String[] sql = new String[sqlList.size()];
			sql = sqlList.toArray(sql);
			ProfiledQuery[] qinfos = new ProfiledQuery[sql.length];
			
			for (int i = 0; i < sql.length; i++) {
				qinfos[i] = profiler.processQuery(DBConnection.trimSqlStatement(sql[i]));
			}
			
			// write out candidates
			File candFile = Configuration.mode.candidatePoolFile();
			ObjectOutputStream out = new ObjectOutputStream(Files.initOutputFile(candFile));
			try {
				out.writeObject(pool);
			} finally {
				out.close();
			}
			
			// write out queries
			File queryFile = Configuration.mode.profiledQueryFile();
			out = new ObjectOutputStream(Files.initOutputFile(queryFile));
			try {
				out.writeObject(qinfos);
			} finally {
				out.close();
			}
			
		} finally {
			Main.tryToCloseConnection(conn);
		}
	}
	
	public static void runOfflineProfiling() throws Exception {
		File workloadFile = Configuration.workloadFile();
		File advisorWorklaodFile = Configuration.advisorWorkloadFile();
		
		DBConnection conn = Main.openConnectionOrExit();
		
		try {
			// get the candidates and profiled queries
			CandidatePool pool = OfflineAnalysis.getCandidates(conn, advisorWorklaodFile);
			ProfiledQuery[] qinfos = OfflineAnalysis.profileQueries(conn, workloadFile, pool);
			
			// write out candidates
			File candFile = Configuration.mode.candidatePoolFile();
			ObjectOutputStream out = new ObjectOutputStream(Files.initOutputFile(candFile));
			try {
				out.writeObject(pool);
			} finally {
				out.close();
			}
			
			// write out queries
			File queryFile = Configuration.mode.profiledQueryFile();
			out = new ObjectOutputStream(Files.initOutputFile(queryFile));
			try {
				out.writeObject(qinfos);
			} finally {
				out.close();
			}
			
			Debug.println();
			Debug.println("wrote " + qinfos.length + " queries");
		} finally {
			Main.tryToCloseConnection(conn);
		}
	}

	public static void runGoodInterventions() throws Exception {
		CandidatePool pool = Main.readCandidatePool();
		Snapshot snapshot = pool.getSnapshot();
		ProfiledQuery[] qinfos = Main.readProfiledQueries();
		int queryCount = qinfos.length;
		Debug.println("read " + queryCount + " queries");
		
		File inputLogFile = Mode.GOOD.inputLogFile();
		Debug.println("Getting log from " + inputLogFile.getAbsolutePath());
		WFALog inputLog = readLog(inputLogFile);
		
		// get the list of recommendations
		BitSet[] recs = new BitSet[queryCount];
		for (int i = 0; i < queryCount; i++) {
			recs[i] = new BitSet();
			recs[i].set(inputLog.getEntry(i).recommendation);
		}

		// print recs to verify
		Debug.println("Recommendations reconstructed from log: ");
		for (BitSet bs : recs) Debug.println(bs);
		Debug.println();
		
		// get the partition in BitSet form
		BitSet[] partitionBitSets = new BitSet[inputLog.getEntry(0).partition.length];
		for (int t = 0; t < partitionBitSets.length; t++) {
			partitionBitSets[t] = new BitSet();
			partitionBitSets[t].set(inputLog.getEntry(0).partition[t]);
		}
		
		IndexPartitions parts = new IndexPartitions(snapshot, partitionBitSets);
		
		// print partitions to verify
		Debug.println("Partitions reconstructed from log: ");
		for (BitSet bs : parts.bitSetArray()) Debug.println(bs);
		Debug.println();

		boolean wfaKeepHistoryOption = false;
		WorkFunctionAlgorithm wfa = new WorkFunctionAlgorithm(parts, wfaKeepHistoryOption);
			
		// run workload
		BitSet[] wfitSchedule = new GoodAdmin(snapshot, recs).getRecs(qinfos, wfa);
		
		WFALog log = WFALog.generateFixed(qinfos, wfitSchedule, snapshot, parts);
		File logFile = Mode.GOOD.logFile();
		writeLog(logFile, log, snapshot);
		processLog(logFile);
		Debug.println();
		Debug.println("wrote log to " + logFile);
		log.dump();
	}
	
	public static void runNoVoting() throws Exception {
		CandidatePool pool = Main.readCandidatePool();
		Snapshot snapshot = pool.getSnapshot();
		ProfiledQuery[] qinfos = Main.readProfiledQueries();
		int queryCount = qinfos.length;
		Debug.println("read " + queryCount + " queries");
		
		// get the list of OPT recommendations
		// also get partitions in BitSet form
		BitSet[] optRecs;
		BitSet[] partitionBitSets;
		{ 
			File optLogFile = Mode.NOVOTE.inputLogFile();
			WFALog optLog = readLog(optLogFile);
			assert queryCount == optLog.entryCount();
			optRecs = new BitSet[queryCount];
			for (int i = 0; i < queryCount; i++) {
				optRecs[i] = new BitSet();
				optRecs[i].set(optLog.getEntry(i).recommendation);
			}
			partitionBitSets = new BitSet[optLog.getEntry(0).partition.length];
			for (int t = 0; t < partitionBitSets.length; t++) {
				partitionBitSets[t] = new BitSet();
				partitionBitSets[t].set(optLog.getEntry(0).partition[t]);
			}
		}
		
		IndexPartitions parts = new IndexPartitions(snapshot, partitionBitSets);
		
		// get the list of WFIT recommendations
		BitSet[] wfitRecs;
		{
			File wfitLogFile = Mode.WFIT.logFile();
			WFALog wfitLog = readLog(wfitLogFile);
			assert queryCount == wfitLog.entryCount();
			wfitRecs = new BitSet[queryCount];
			for (int i = 0; i < queryCount; i++) {
				wfitRecs[i] = new BitSet();
				wfitRecs[i].set(wfitLog.getEntry(i).recommendation);
			}
		}
		
		assert wfitRecs.length == optRecs.length;
		
		WFALog log = WFALog.generateDual(qinfos, optRecs, wfitRecs, snapshot, parts);
		File logFile = Mode.NOVOTE.logFile();
		writeLog(logFile, log, snapshot);
		processLog(logFile);
		Debug.println();
		Debug.println("wrote log to " + logFile);
		log.dump();
	}
	
	public static void runBadInterventions() throws Exception {
		CandidatePool pool = Main.readCandidatePool();
		Snapshot snapshot = pool.getSnapshot();
		ProfiledQuery[] qinfos = Main.readProfiledQueries();
		int queryCount = qinfos.length;
		Debug.println("read " + queryCount + " queries");
		
		File inputLogFile = Mode.BAD.inputLogFile();
		Debug.println("Getting log from " + inputLogFile.getAbsolutePath());
		WFALog inputLog = readLog(inputLogFile);
		
		// get the list of recommendations
		BitSet[] recs = new BitSet[queryCount];
		for (int i = 0; i < queryCount; i++) {
			recs[i] = new BitSet();
			recs[i].set(inputLog.getEntry(i).recommendation);
		}

		// print recs to verify
		Debug.println("Recommendations reconstructed from log: ");
		for (BitSet bs : recs) Debug.println(bs);
		Debug.println();
		
		// get the partition in BitSet form
		BitSet[] partitionBitSets = new BitSet[inputLog.getEntry(0).partition.length];
		for (int t = 0; t < partitionBitSets.length; t++) {
			partitionBitSets[t] = new BitSet();
			partitionBitSets[t].set(inputLog.getEntry(0).partition[t]);
		}
		
		IndexPartitions parts = new IndexPartitions(snapshot, partitionBitSets);
		
		// print partitions to verify
		Debug.println("Partitions reconstructed from log: ");
		for (BitSet bs : parts.bitSetArray()) Debug.println(bs);
		Debug.println();

		boolean wfaKeepHistoryOption = false;
		WorkFunctionAlgorithm wfa = new WorkFunctionAlgorithm(parts, wfaKeepHistoryOption);
			
		// run workload
		BitSet[] wfitSchedule = new BadAdmin(snapshot, recs).getRecs(qinfos, wfa);
		
		WFALog log = WFALog.generateFixed(qinfos, wfitSchedule, snapshot, parts);
		File logFile = Mode.BAD.logFile();
		writeLog(logFile, log, snapshot);
		processLog(logFile);
		Debug.println();
		Debug.println("wrote log to " + logFile);
		log.dump();
	}
	
	public static void runSlow(boolean vote) throws Exception {
		int maxNumIndexes = Configuration.maxHotSetSize;
		int maxNumStates = Configuration.maxNumStates;
		int lag = Configuration.slowAdminLag;
		
		CandidatePool pool = Main.readCandidatePool();
		ProfiledQuery[] qinfos = Main.readProfiledQueries();
		int queryCount = qinfos.length;
		Debug.println("read " + queryCount + " queries");
		
		Snapshot snapshot = pool.getSnapshot();
		IndexPartitions parts = OfflineAnalysis.getPartition(snapshot, qinfos, maxNumIndexes, maxNumStates);

		boolean wfaKeepHistoryOption = false;
		WorkFunctionAlgorithm wfa = new WorkFunctionAlgorithm(parts, wfaKeepHistoryOption);
			
		// run workload
		BitSet[] sched = new SlowAdmin(snapshot, lag, vote).getRecs(qinfos, wfa);
		
		WFALog log = WFALog.generateFixed(qinfos, sched, snapshot, parts);
		File logFile = (vote) ? Mode.SLOW.logFile() : Mode.SLOW_NOVOTE.logFile();
		writeLog(logFile, log, snapshot);
		processLog(logFile);
		Debug.println();
		Debug.println("wrote log to " + logFile);
		log.dump();
	}
	
	public static void runAutomatic() throws Exception {
		Selector selector = new Selector();
		
		ProfiledQuery[] queries = Main.readProfiledQueries();
		int queryCount = queries.length;
		Debug.println("read "+queryCount+" queries");
		
		AnalyzedQuery[] qinfos = new AnalyzedQuery[queryCount];
		BitSet[] recs = new BitSet[queryCount];
		for (int q = 0; q < queryCount; q++) {
			ProfiledQuery query = queries[q];
			Debug.println("issuing query: " + query);
			
			// analyze the query and get the recommendation
			qinfos[q] = selector.analyzeQuery(query);
			recs[q] = new BitSet();
			for (DB2Index idx : selector.getRecommendation()) 
				recs[q].set(idx.internalId());
		}
		
		Snapshot lastCandidateSet = queries[queryCount-1].candidateSet;
		
		WFALog log = WFALog.generateDynamic(qinfos, recs);
		File logFile = Configuration.mode.logFile();
		Main.writeLog(logFile, log, lastCandidateSet);
		processLog(logFile);
		ObjectOutputStream out = new ObjectOutputStream(Files.initOutputFile(logFile));
		try {
			out.writeObject(log);
			out.writeObject(lastCandidateSet);
		} finally {
			out.close();
		}
		
		log.dump();
		for (DB2Index index: lastCandidateSet) {
			System.out.println(index.creationText());
		}
	}

	private static void runLogging(Mode mode) throws Exception {
		processLog(mode.logFile());
		if (mode == Mode.OPT) {
			try {
				Debug.print("Trying to get minimum WF values... ");
				processWfFile(Configuration.minWfFile());
				Debug.println("done");
			} catch (java.io.FileNotFoundException e) {
				Debug.println("file not found");
			}
		}
	}
	
	private static void processLog(File logFile) throws Exception {
		WFALog log = readLog(logFile);

		File logTxtFile = new File(logFile.getAbsolutePath()+".txt");
		PrintStream out = new PrintStream(new FileOutputStream(logTxtFile));
		try { log.dumpPerformance(out); } 
		finally { out.close(); }
		
		File historyTxtFile = new File(logFile.getAbsolutePath()+"_history.txt");
		out = new PrintStream(new FileOutputStream(historyTxtFile));
		try { log.dumpHistory(out); } 
		finally { out.close(); }
	}
	
	private static void processWfFile(File wfFile) throws Exception {
		ObjectInputStream in = new ObjectInputStream(new FileInputStream(wfFile));
		try {
			double[] minWfValues = (double[]) in.readObject();
			
			File wfTxtFile = new File(wfFile.getAbsolutePath()+".txt");
			PrintStream out = new PrintStream(new FileOutputStream(wfTxtFile));
			try {
				for (double val : minWfValues)
					out.println(val);
			} finally {	out.close(); }
		} finally {
			in.close(); // closes underlying stream
		}		
	}
}
