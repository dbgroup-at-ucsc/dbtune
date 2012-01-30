package interaction;

import java.io.File;

import interaction.cand.Generation;
import interaction.cand.Generation.Strategy;
import interaction.ibg.AnalysisMode;

public abstract class Configuration {
	// Experiment settings
	// These are defaults
	// Actual values may be taken from command line
	public static Generation.Strategy candidates = Generation.Strategy.UNION_OPTIMAL;
	public static double interactionThreshold = 0.01;
	public static String subdir = "tpch"; // subdirectory of resultsRoot, see below
	/*
	// below this line should not need much changing
	public static String dbName = "KARLSCH";
	public static String userName = "karlsch";
	public static String password = null;
	public static String url = "jdbc:db2://localhost:48459/karlsch";
	public static String driverClass = "com.ibm.db2.jcc.DB2Driver";
	public static String db2Advis = "/Users/karlsch/sqllib/bin/db2advis";	
	// File management 
	private static File root = new File("/Users/karlsch/school/chameleon-dbms/bengraph/results");
	*/
	public static String dbName = "ONEGB";
    public static String userName = "db2inst1";
    public static String password = "db2inst1admin";
    public static String url = "jdbc:db2://localhost:50000/ONEGB";
    public static String driverClass = "com.ibm.db2.jcc.DB2Driver";
    public static String db2Advis = "/home/db2inst1/sqllib/bin/db2advis";
    
    // File management
    private static File root = new File("/home/tqtrung/previous_source_code/bengraph/results/");
    
	private static File resultsDir() {
		return new File (root, subdir);
	}
	private static File outputDir() {
		return new File(resultsDir(), "output");
	}
	private static File inputDir() {
		return new File(resultsDir(), "input");
	}
	public static File queryListFile() {
		return new File(inputDir(), "filelist.txt");
	}
	public static File workloadFile() {
		return new File(outputDir(), "workload.txt");
	}
//	public static File advisorFile(Generation.Strategy s) {
//		return new File(outputDir(), "advisor-" + s.nickname + ".txt");
//	}
	public static File candidateFile(Generation.Strategy s) {
		return new File(outputDir(), "candidate-" + s.nickname);
	}
	public static File analysisFile(Generation.Strategy s, AnalysisMode mode) {
		return new File(outputDir(), "analysis-" + s.nickname + "-" + mode);
	}
	public static File ibgFile(Generation.Strategy s) {
		return new File(outputDir(), "ibg-" + s.nickname);
	}
	public static File logFile(Strategy s, AnalysisMode m, double t) {
		return new File(outputDir(), "log-" + s.nickname + "-" + m + "-" + t + ".txt");
	}
}
