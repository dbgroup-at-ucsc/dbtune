package interaction;

import static interaction.cand.Generation.Strategy.*;
import interaction.cand.Generation;
import interaction.db.*;
import interaction.ibg.*;
import static interaction.ibg.AnalysisMode.PARALLEL;
import static interaction.ibg.AnalysisMode.SERIAL;
import interaction.ibg.log.AnalysisLog;
import interaction.ibg.log.BasicLog;
import interaction.ibg.log.InteractionLogger;
import interaction.ibg.parallel.*;
import interaction.ibg.serial.*;
import interaction.util.Files;
import interaction.workload.*;

import java.io.File;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.PrintWriter;
import java.sql.SQLException;

/*
 * This is where the interesting things start happening, after we have
 * connected to the database and collected the workload queries
 */
public class AnalysisMain {
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

	private static void runSteps() throws SQLException, IOException, ClassNotFoundException {
		// Connect to database
		DBConnection conn = Main.openConnection();
		try {
			SQLWorkload workload = Main.getWorkload();
//			analyze(conn, workload, UNION_OPTIMAL);
			analyze(conn, workload, OPTIMAL_1C);
//			analyze(conn, workload, FULL_BUDGET);
//			analyze(conn, workload, HALF_BUDGET);
		} finally {
			conn.commit();
			conn.close();
		}
	}
	
	private static void analyze(DBConnection conn, SQLWorkload workload, Generation.Strategy strategy) throws IOException, ClassNotFoundException, SQLException {
		InteractionLogger logger;
		SerialIndexBenefitGraph[] ibgs;
		File candidateFile = Configuration.candidateFile(strategy);
		DB2IndexSet candidateSet = (DB2IndexSet) Files.readObjectFromFile(candidateFile);

		conn.fixCandidates(candidateSet);
		logger = new InteractionLogger(conn, candidateSet);	
		
		// reset logger and analyze serially
		// then write out the ibgs and analysis
		logger.reset();
		ibgs = analyzeSerial(conn, workload, candidateSet, logger);
		writeAnalysis(strategy, SERIAL, logger.getBasicLog());
		writeIBGs(ibgs, strategy);
	
		
		// test
		PrintWriter out = new PrintWriter(System.out);
		try {
			System.out.println("Serial log:");
			BasicLog serial1 = (BasicLog)Files.readObjectFromFile(Configuration.analysisFile(strategy, SERIAL));
			AnalysisLog serial2 = serial1.getAnalysisLog(0.1);
			serial2.output(out);
			
			// reset logger and analyze in parallel
			logger.reset();
			analyzeParallel(conn, workload, candidateSet, logger);
			writeAnalysis(strategy, PARALLEL, logger.getBasicLog());

			System.out.println("Parallel log:");
			BasicLog parallel1 = (BasicLog)Files.readObjectFromFile(Configuration.analysisFile(strategy, PARALLEL));
			AnalysisLog parallel2 = parallel1.getAnalysisLog(0.1);
			parallel2.output(out);
		} finally {
			out.close();
		}
		
		
		

	}

	public static SerialIndexBenefitGraph[] analyzeSerial(DBConnection conn, SQLWorkload xacts, DB2IndexSet candidateSet, InteractionLogger logger) 
	throws SQLException {
		SerialIndexBenefitGraph[] ibgs;
		int i;
		
		logger.startTimer();
		
		/* analyze queries one at a time, combining their interactions */
		ibgs = new SerialIndexBenefitGraph[xacts.transactionCount()];
		i = 0;
		for (SQLTransaction xact : xacts) {
			SerialIndexBenefitGraph ibg = SerialIndexBenefitGraph.build(conn, new SQLWorkload(xact), candidateSet);
			SerialIBGAnalyzer analyzer = new SerialIBGAnalyzer(ibg);
			analyzer.doAnalysis(logger);
			ibgs[i++] = ibg;
		}
		
		//DB2GreedyScheduler.schedule(candidateSet.bitSet(), new SerialIndexBenefitGraph.ScheduleInfo(ibgs)); 

		return ibgs;
	}
	
	public static IndexBenefitGraph[] analyzeParallel(DBConnection conn, SQLWorkload xacts, DB2IndexSet candidateSet, InteractionLogger logger)
	throws SQLException {
		IndexBenefitGraph[] ibgs;
		IBGAnalyzer[] analyzers;
		int xactCount;
		int i;
		
		logger.startTimer();
		
		// initialize IBGs and Analyzers, separate one for each transaction
		xactCount = xacts.transactionCount();
		ibgs = new IndexBenefitGraph[xactCount];
		analyzers = new IBGAnalyzer[xactCount];
		i = 0;
		for (SQLTransaction xact : xacts) {
			ibgs[i] = new IndexBenefitGraph(conn, new SQLWorkload(xact), candidateSet);
			analyzers[i] = new IBGAnalyzer(ibgs[i]);
			i++;
		}
		
		// start the threads
		Thread analysisThread = new Thread(new RRAnalysisThread(analyzers, candidateSet, logger));
		Thread constructionThread = new Thread(new RRConstructionThread(ibgs));
		constructionThread.start();
		analysisThread.start();
		
		while (true) {
			try {
				analysisThread.join();
				constructionThread.join();
				break;
			} catch (InterruptedException e) {
				System.err.println("join() interrupted");
			}
		}

		//logger.print();
		//new interaction.ibg.multi.IBGPrinter().print(ibgs[0]);
		//multiSchedules(candidateSet, ibgs, bank);
		
		return ibgs;
	}
	
	private static void writeAnalysis(Generation.Strategy strategy, AnalysisMode mode, BasicLog log) throws IOException {
		File analysisFile = Configuration.analysisFile(strategy, mode);
		ObjectOutputStream out = new ObjectOutputStream(Files.initOutputFile(analysisFile));
		try {
			out.writeObject(log);
		} finally {
			out.close();
		}
	}
	
	private static void writeIBGs(SerialIndexBenefitGraph[] ibgs, Generation.Strategy strategy) throws IOException {
		File ibgFile = Configuration.ibgFile(strategy);
		ObjectOutputStream out = new ObjectOutputStream(Files.initOutputFile(ibgFile));
		try {
			out.writeObject(ibgs);
		} finally {
			out.close();
		}
	}
	
//	public static void analyze(DBConnection conn, SQLWorkload xacts) 
//	throws SQLException, InterruptedException {
//		DB2IndexSet candidateSet;
//		
//		// Load the index set
//		candidateSet = UnionOptimal.getCandidates(conn, xacts);
//		conn.fixCandidates(candidateSet);
//		System.println(candidateSet);
//      System.println();
//		System.gc();
//		
//		startMillis = System.currentTimeMillis();
//		
////		analyzeTwoPhase(conn, xacts, candidateSet);
////		analyzeAnytime(conn, xacts, candidateSet);
//		analyzeMulti(conn, xacts, candidateSet);
//
//		long endMillis = System.currentTimeMillis();
//		Debug.println("total time for IBG construction and analysis: " + ((endMillis-startMillis)/1000.0) + " seconds");
//		Debug.println("Total whatif calls: " + conn.whatifCount);
//	}
//	
//	public static void analyzeMulti(DBConnection conn, SQLWorkload xacts, DB2IndexSet candidateSet)
//	throws SQLException {
//		InteractionBank bank;
//		IndexBenefitGraph[] ibgs;
//		IBGAnalyzer[] analyzers;
//		int xactCount;
//		int i;
//		
//		// initialize IBGs and Analyzers, separate one for each transaction
//		xactCount = xacts.transactionCount();
//		bank = new InteractionBank(candidateSet);
//		ibgs = new IndexBenefitGraph[xactCount];
//		analyzers = new IBGAnalyzer[xactCount];
//		i = 0;
//		for (SQLTransaction xact : xacts) {
//			ibgs[i] = new IndexBenefitGraph(conn, new SQLWorkload(xact), candidateSet);
//			analyzers[i] = new IBGAnalyzer(ibgs[i]);
//			i++;
//		}
//		
//		// start the threads
//		Thread analysisThread = new Thread(new RRAnalysisThread(analyzers, candidateSet, bank));
//		Thread constructionThread = new Thread(new RRConstructionThread(ibgs));
//		constructionThread.start();
//		analysisThread.start();
//		
//		while (true) {
//			try {
//				analysisThread.join();
//				constructionThread.join();
//				break;
//			} catch (InterruptedException e) {
//				System.err.println("join() interrupted");
//			}
//		}
//
//		bank.print();
//		//new interaction.ibg.multi.IBGPrinter().print(ibgs[0]);
//		multiSchedules(candidateSet, ibgs, bank);
//	}
//	
//	public static void multiSchedules(DB2IndexSet candidateSet, IndexBenefitGraph[] ibgs, InteractionBank bank) {
//		IndexSchedule schedule;
//		long start, end;
//		BitSet rootBitSet = candidateSet.bitSet();
//		
//		interaction.ibg.parallel.IBGMonotonicEnforcer monotonize = new interaction.ibg.parallel.IBGMonotonicEnforcer();
//		for (IndexBenefitGraph ibg : ibgs) {
//			monotonize.fix(ibg);
//		}
//		
//		start = System.currentTimeMillis();
//		schedule = DB2GreedyScheduler.schedule(rootBitSet, ibgs);
//		end = System.currentTimeMillis();
//		System.out.println();
//		System.out.print("DB2GreedyScheduler: ");
//		schedule.print();
//		System.out.println("  (" + ((end-start)/1000.0) + " seconds)");
//		
//		start = System.currentTimeMillis();
//		schedule = GreedyScheduler.schedule(rootBitSet, ibgs);
//		end = System.currentTimeMillis();
//		System.out.println();
//		System.out.print("GreedyScheduler: ");
//		schedule.print();
//		System.out.println("  (" + ((end-start)/1000.0) + " seconds)");
//		
//		start = System.currentTimeMillis();
//		schedule = ReverseGreedyScheduler.schedule(rootBitSet, ibgs);
//		end = System.currentTimeMillis();
//		System.out.println();
//		System.out.print("ReverseGreedyScheduler: ");
//		schedule.print();
//		System.out.println("  (" + ((end-start)/1000.0) + " seconds)");
//		
//		start = System.currentTimeMillis();
//		schedule = PartitionedScheduler.schedule(rootBitSet, ibgs, bank);
//		end = System.currentTimeMillis();
//		System.out.println();
//		System.out.print("PartitionedScheduler: ");
//		schedule.print();
//		System.out.println("  (" + ((end-start)/1000.0) + " seconds)");
//		
////		start = System.currentTimeMillis();
////		schedule = OptimalScheduler.schedule(rootBitSet, ibgs);	
////		end = System.currentTimeMillis();
////		System.out.println();
////		System.out.print("OptimalScheduler: ");
////		schedule.print();
////		System.out.println("  (" + ((end-start)/1000.0) + " seconds)");
//	}
//	
////	public static void analyzeAnytime(DBConnection conn, SQLWorkload xacts, DB2IndexSet candidateSet) 
////	throws SQLException, InterruptedException {
////		AnytimeIBG ibg;
////		AnytimeAnalyzer analyzer;
////		Thread th;
////		
////		// create the object that will traverse the graph for us
////		analyzer = new AnytimeAnalyzer(candidateSet);
////		
////		/* analyze queries one at a time, separating their interactions */
////		for (SQLTransaction xact : xacts) {
////			// create IBG
////			ibg = new AnytimeIBG();
////			// start the parallel analysis on fresh analyzer
////			analyzer.reset();
////			th = analyzer.process(ibg);
////			// start building 
////			ibg.build(conn, new SQLWorkload(xact), candidateSet, analyzer);
////			System.out.println("finished building");
////			th.join();
////			analyzer.print();
////			//ibg.print();
////		}
////		
////		
////		
//////		/* analyze queries one at a time, but combine their interactions */
//////		for (SQLTransaction xact : xacts) {
//////			// create IBG 
//////			ibg = new AnytimeIBG();
//////			// start the parallel analysis without resetting
//////			th = analyzer.process(ibg);
//////			// start building 
//////			ibg.build(conn, new SQLWorkload(xact), candidateSet, analyzer);
//////		    th.join();
//////		}
//////		analyzer.print();
//////		
//////		
//////		
//////		/* analyze whole workload at once */
//////		// create IBG 
//////		ibg = new AnytimeIBG();
//////		// start the parallel analysis
//////		analyzer.process(ibg);
//////		// start building 
//////		ibg.build(conn, xacts, candidateSet, analyzer);
//////		th.join();
//////		analyzer.print();
////	}
//	
//	public static void analyzeTwoPhase(DBConnection conn, SQLWorkload xacts, DB2IndexSet candidateSet) 
//	throws SQLException {
//		SerialIndexBenefitGraph ibg;
//		SerialIBGAnalyzer analyzer;
//		InteractionBank bank;
//		
//		/* analyze queries one at a time, combining their interactions */
//		bank = new InteractionBank(candidateSet);
//		for (SQLTransaction xact : xacts) {
//			ibg = new SerialIndexBenefitGraph(conn, new SQLWorkload(xact), candidateSet);
//			analyzer = new SerialIBGAnalyzer(ibg);
//			analyzer.doAnalysis(bank);	
//		}
//	}
}
