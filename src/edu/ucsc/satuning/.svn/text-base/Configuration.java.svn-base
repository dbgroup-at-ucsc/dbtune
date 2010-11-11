package edu.ucsc.satuning;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import java.io.File;

public abstract class Configuration {
	// Experiment settings
	// These are defaults
	// Actual values may be taken from command line
	public static double interactionThreshold = 0.01;
	
	//public static String subdirName = null; // name of test, used to determine a subdir name, see below
	//public static String dirName = null;
	
	// what algorithm(s) to run
	public static Mode mode = null;
	
	// what dbms to use
	public static DBMS dbms = DBMS.PG;
	
	// where to find the workload (defaults to directory/workload.sql)
	public static String workloadString = null;
	
	// is this the logging post-processing?
	public static boolean logging = false;
	
	// configurable options
	public static int maxHotSetSize = 40;
	public static int maxNumStates = 12345;
	
	//public static boolean onlineCandidates = true;
	public static int queryQueueCapacity = 100;
	public static int partitionIterations = 10;
	public static int indexStatisticsWindow = 100;
	public static int slowAdminLag = 5;
	
	// below this line should not need much configuration
    // todo(Huascar) what is this? and where can we find it?
	public static String db2Advis = "/home/karlsch/sqllib/bin/db2advis";
	
	// File management 
	public static File root = new File("/home/karlsch/satuning/pgtests");
	private static File directory = null; 

	private static File subdir() {
		return directory;
	}
	
	public static File workloadFile() {
		if (workloadString != null)
			return new File(workloadString);
		return new File(subdir(), "workload.sql");
	}
//	public static File advisorWorkloadFile() {
//		return new File(subdir(), "advisor-workload.sql");
//	}
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
	
	public static void processArgs(String[] args) {
		Options o = new Options();
		
		o.addOption("subdir", true, "subdirectory of " + root);
		o.addOption("dir", true, "absolute path of experiment (alternative to 'subdir' option)");
		o.addOption("mode", true, "one of {"+Mode.options()+"}");
		o.addOption("hot", true, "max hot set size");
		o.addOption("states", true, "max number of states");
		o.addOption("log", false, "flag to do log post-processing");
		o.addOption("lag", true, "lag of the slow admin");
		o.addOption("dbms", true, "dbms to use, e.g., pg, ibm");
		o.addOption("workload", true, "absolute path to workload");
		
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
	        
	        String subdirName = line.getOptionValue("subdir");
	        String dirName = line.getOptionValue("dir");
	        if (subdirName == null) {
	        	if (dirName == null) {
	        		System.out.println("subdir or dir option is required");
	        		return false;
	        	}
	        	directory = new File(dirName);
	        }
	        else if (dirName != null) {
	        	System.out.println("subdir and dir may not both be specified");
	        	return false;
	        }
	        else {
	        	directory = new File(root, subdirName);
	        }
	        	
	        System.out.println("using directory " + directory);
	        
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
	        
	        String dbmsString = line.getOptionValue("dbms");
	        if (dbmsString != null) {
	        	dbms = DBMS.parseArgument(dbmsString);
	        	if (dbms == null)
	        		throw new ParseException("DBMS " + dbmsString + " not recognized");
	        }
	        else {
	        	System.out.println("dbms option is required");
	        	return false;
	        }
	        	
	        workloadString = line.getOptionValue("workload");
	    }
	    catch (ParseException exp) {
	        // oops, something went wrong
	        System.err.println("Parsing failed.  Reason: " + exp.getMessage());
	        return false;
	    }

		return true;
	}
}
