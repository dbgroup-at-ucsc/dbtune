package satuning;

import java.io.File;

public abstract class Configuration {
	// Experiment settings
	// These are defaults
	// Actual values may be taken from command line
	public static double interactionThreshold = 0.01;
	
	public static String subdirName = null; // name of test, used to determine a subdir name, see below
	
	// what algorithm(s) to run
	public static Mode mode = null;
	
	// is this the logging post-processing?
	public static boolean logging = false;

	// config defaults
	public static final int defaultHotSetSize = 40;
	public static final int defaultNumStates = 500;	

	// configurable options
	public static int maxHotSetSize = defaultHotSetSize;
	public static int maxNumStates = defaultNumStates;
	
	//public static boolean onlineCandidates = true;
	public static int queryQueueCapacity = 100;
	public static int partitionIterations = 10;
	public static int indexStatisticsWindow = 100;
	public static int slowAdminLag = 5;

        // below this line should not need much changing
        public static String dbName = "KARLSCH";
        public static String userName = "karlsch";
        public static String password = null;
        public static String url = "jdbc:db2://localhost:48459/karlsch";
        public static String driverClass = "com.ibm.db2.jcc.DB2Driver";
        public static String db2Advis = "/Users/karlsch/sqllib/bin/db2advis";

        // File management
        private static File root = new File("/Users/karlsch/school/chameleon-dbms/satuning/tests");

	public static File passwordFile = new File(root, "password");

	private static File subdir() {
		return new File(root, subdirName);
	}
	
	public static File workloadFile() {
		return new File(subdir(), "workload.sql");
	}
	public static File advisorWorkloadFile() {
		return new File(subdir(), "advisor-workload.sql");
	}
	public static File logFile(String string) {
		return new File(subdir(), "logfile_"+string);		
	}
	public static File profiledQueryFile(String string) {
		return new File(subdir(), "profiledQueries-"+string);	
	}
	public static File minWfFile() {
		return new File(subdir(), "minWfValues_"+Configuration.maxHotSetSize+"_"+Configuration.maxNumStates);
	}
	public static File candidatePoolFile(String string) {
		return new File(subdir(), "candidatePool-"+string);	
	}
	
    /*
	public static void processArgs(String[] args) {
		Options o = new Options();
		
		o.addOption("subdir", true, "subdirectory of " + root);
		o.addOption("mode", true, "one of {"+Mode.options()+"}");
		o.addOption("hot", true, "max hot set size");
		o.addOption("states", true, "max number of states");
		o.addOption("log", false, "flag to do log post-processing");
		o.addOption("lag", true, "lag of the slow admin");
		
		if (!processArgs(args, o)) {
			HelpFormatter help = new HelpFormatter();
			help.printHelp("semi-automatic tuning", o);
			System.exit(0);
		}
	}
	
	private static boolean processArgs(String[] args, Options o) {
	    // create the parser
	    CommandLineParser parser = new GnuParser();
	    try {
	        // parse the command line arguments
	        CommandLine line = parser.parse(o, args);
	        
	        subdirName = line.getOptionValue("subdir");
	        if (subdirName == null) {
	        	System.out.println("subdir option is required");
	        	return false;
	        }
	        System.out.println("using subdirectory " + subdirName);
	        
	        String modeString = line.getOptionValue("mode");
	        if (modeString == null) {
	        	System.out.println("mode option is required");
	        	return false;
	        }
	        else {
	        	// will throw ParseException if not a valid string
	        	mode = Mode.parseMode(modeString);
	        }
	        System.out.println("using mode " + mode);
	        
	        String hotString = line.getOptionValue("hot");
	        if (hotString != null) {
	        	try {
	        		maxHotSetSize = Integer.parseInt(hotString);
	        	} catch (Throwable e) {
	        		throw new ParseException(e.getMessage());
	        	}
	        }
	        System.out.println("maxHotSetSize = " + maxHotSetSize);
	        
	        String statesString = line.getOptionValue("states");
	        if (statesString != null) {
	        	try {
	        		maxNumStates = Integer.parseInt(statesString);
	        	} catch (Throwable e) {
	        		throw new ParseException(e.getMessage());
	        	}
	        }
	        System.out.println("maxNumStates = " + maxNumStates);
	        
	        logging = line.hasOption("log");
	        if (logging)
	        	System.out.println("will do log processing");
	        
	        String lagString = line.getOptionValue("lag");
	        if (lagString != null) {
	        	try {
	        		slowAdminLag = Integer.parseInt(lagString);
	        	} catch (Throwable e) {
	        		throw new ParseException(e.getMessage());
	        	}
	        }
	        System.out.println("slowAdminLag = " + slowAdminLag);
	    }
	    catch (ParseException exp) {
	        // oops, something went wrong
	        System.err.println("Parsing failed.  Reason: " + exp.getMessage());
	        return false;
	    }

		return true;
	}
    */
}
