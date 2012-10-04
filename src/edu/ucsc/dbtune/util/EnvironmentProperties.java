package edu.ucsc.dbtune.util;

import java.util.ArrayList;
import java.util.List;

/**
 * Set of properties used in the DBTune API. They are used to introspect the environment where the 
 * API is running. The {@code FILE} field indicates where the settings are supposed to be read from.
 *
 * @author Huascar A. Sanchez
 * @author Ivo Jimenez
 */
public final class EnvironmentProperties
{
    // XXX: Note to devs.
    //
    // Each set of properties is logically grouped (using 3 empty lines to separate groups; no space 
    // between properties from the same group) based on what they refer to, eg. connectivity, 
    // supported dbms, optimizers available, etc..
    // 
    // For some groups, the addition of a new property may need to be reflected in other classes of 
    // the API. When this is the case, there will be a note indicating the list of class#method to 
    // update.



    /**
     * Name of the configuration properties file.
     */
    public static final String FILE;

    static {
        if (System.getenv("DBTUNECONFIG") == null)
            FILE = System.getProperty("user.dir") + "/config/dbtune.cfg";
        else
            FILE = System.getenv("DBTUNECONFIG");
    }



    /**
     * Developer's DBMS username.
     */
    public static final String USERNAME = "username";
    /**
     * Developer's DBMS password.
     */
    public static final String PASSWORD = "password";
    /**
     * JDBC connection url.
     */
    public static final String JDBC_URL = "jdbc.url";



    /**
     * Fully qualified path to JDBC's {@code Driver} class.
     */
    public static final String JDBC_DRIVER = "driver";
    /**
     * DB2 driver class path.
     */
    public static final String DB2 = "com.ibm.db2.jcc.DB2Driver";
    /**
     * MySQL driver class path.
     */
    public static final String MYSQL = "com.mysql.jdbc.Driver";
    /**
     * PG driver class path.
     */
    public static final String PG = "org.postgresql.Driver";
    /**
     * List of vendors that the API supports.
     */
    public static final List<String> SUPPORTED_VENDORS;

    static {
        SUPPORTED_VENDORS = new ArrayList<String>();

        SUPPORTED_VENDORS.add(DB2);
        SUPPORTED_VENDORS.add(MYSQL);
        SUPPORTED_VENDORS.add(PG);
    }



    // XXX: Note to devs
    // 
    // if a new type of optimizer is added, the Wiki entry 'core-configuration-file' should be 
    // updated accordingly.
    /**
     * Type of optimizer to use.
     */
    public static final String OPTIMIZER = "optimizer";
    /**
     * DBMS optimizer.
     */
    public static final String DBMS = "dbms";
    /**
     * INUM optimizer.
     */
    public static final String INUM = "inum";
    /**
     * IBG optimizer.
     */
    public static final String IBG = "ibg";
    /**
     * List of supported optimizers.
     */
    public static final List<String> SUPPORTED_OPTIMIZERS;

    static {
        SUPPORTED_OPTIMIZERS = new ArrayList<String>();

        SUPPORTED_OPTIMIZERS.add(DBMS);
        SUPPORTED_OPTIMIZERS.add(IBG);
        SUPPORTED_OPTIMIZERS.add(INUM);
    }


    /**
     * A list of space budget, in megabytes, for physical design
     */
    public static final String SPACE_BUDGET = "space.budget";

    /**
     * Type of candidate generator to use. Two generators can be specified (separating them with 
     * comma) if the one accepts another generator as a construction parameter. By convention, a 
     * generator in the right is the parameter of the one in the left.
     */
    public static final String CANDIDATE_GENERATOR = "candidate.generator";
    /**
     * @see OptimizerCandidateGenerator
     */
    //public static final String OPTIMIZER = "optimizer"; // already defined above
    /**
     * @see OneColumnCandidateGenerator
     */
    public static final String ONE_COLUMN = "one.column";
    /**
     * @see PowerSetOptimalCandidateGenerator
     */
    public static final String POWERSET = "powerset";
    /**
     * @see DB2AdvisorCandidateGenerator
     */
    public static final String DB2ADVISOR = "db2advisor";
    

    /**
     * Folder for previously defined workloads. By convention, this folder contains many sub-folders 
     * where each one corresponds to one workload. Each workload folder contains at least two files:
     *
     *  * create.sql
     *  * workload.sql
     * 
     * where the {@code ddl.sql} file contains the schema of the database and {@code workload.sql} 
     * contains SQL statements.
     */
    public static final String WORKLOADS_FOLDERNAME = "workloads.dir";
    /**
     *  Folder for storing temporary file that will be deleted eventually.
     */
    public static final String TEMP_DIR = "temp.dir";


    // ALGORITHMS
    /** a greedy algorithm. */
    public static final String GREEDY = "greedy";
    /** an exhaustive or brute-force algorithm. */
    public static final String EXHAUSTIVE = "exhaustive";



    // EVALUATION
    /** lazy evaluation of a computation. */
    public static final String LAZY = "lazy";
    /** eager evaluation of a computation. */
    public static final String EAGER = "eager";


    
    // INUM
    /** Space computation option. */
    public static final String INUM_SPACE_COMPUTATION = "inum.space.computation";
    /** Space computation option. */
    public static final String INUM_MATCHING_STRATEGY = "inum.matching.strategy";
    /** cache for slots? */
    public static final String INUM_SLOT_CACHE = "inum.slot.cache";
    /** inum-specific algorithm type. */
    public static final String NONE_MIN_MAX = "none.min.max";



    // WFIT
    /**
     * Specifies an upper bound on the number of indexes that are monitored by an instance of WFA 
     * and is used within function {@code chooseCands} (as referenced in page 169 (Figure 6.5) of 
     * Schnaitter's thesis), and implemented in {@link 
     * edu.ucsc.dbtune.advisor.wfit.WorkFunctionAlgorithm#getRecommendation}.
     *
     * @see edu.ucsc.dbtune.advisor.wfit.WorkFunctionAlgorithm#getRecommendation
     */
    public static final String WFIT_MAX_NUM_INDEXES = "max.number.of.indexes";
    /**
     * Specifies an upper bound on the number of configurations tracked by WFIT and is read from 
     * function {@code chooseCands} (as referenced in page 169 (Figure 6.5) of Schnaitter's thesis), 
     * and implemented in {@link 
     * edu.ucsc.dbtune.advisor.wfit.WorkFunctionAlgorithm#getRecommendation}.
     *
     * @see edu.ucsc.dbtune.advisor.wfit.WorkFunctionAlgorithm#getRecommendation
     */
    public static final String WFIT_MAX_NUM_STATES = "max.number.of.states";
    /**
     * XXX document.
     */
    public static final String WFIT_NUM_PARTITION_ITERATIONS = "num.partition.iterations";
    /**
     * XXX document.
     */
    public static final String WFIT_INDEX_STATISTICS_WINDOW = "index.statistics.window";

    /**
     * The maximum time (in seconds) that is set for the CPLEX solver to run
     */
    public static final String CPLEX_MAX_TIME = "cplex.max.time";
    
    /**
     * The error tolerance (in percentage) scale that allows the CPLEX solver to stop
     * whenever the solution is within this given factor of the optimal value 
     */
    public static final String CPLEX_MAX_EP_GAP = "cplex.max.ep.gap"; 
    
    /**
     * The boolean value is turned on/off to show the output
     *  
     */
    public static final String CPLEX_SHOW_OUTPUT = "cplex.show.output";
    
    /**
     * The number of replicas to deploy divergent index tuning
     *  
     */
    public static final String NUMBER_OF_REPLICA = "number.of.replica";
    
    /**
     * Show optimizer cost
     *  
     */
    public static final String SHOW_OPTIMIZER_COST = "optimizer.caculate.cost";
    
    /**
     * List of space budgets (in MB) for physical designs
     *  
     */
    public static final String LIST_SPACE_BUDGET = "list.space.budget";
    
    /**
     * List of imbalance constraints
     *  
     */
    public static final String IMBALANCE_CONSTRAINT = "imbalance.constraint";
    
    /**
     * List of imbalance factors
     *  
     */
    public static final String IMBALANCE_FACTOR = "imbalance.factor";
    
    public static final String QUERY_IMBALANCE = "query.imbalance";
    public static final String NODE_IMBALANCE = "node.imbalance";
    public static final String FAILURE_IMBALANCE = "failure.imbalance";
    
    /**
     * Adaptive DIVBIP parameters
     */
    public static final String WINDOW_DURATION = "window.duration";
    public static final String NUMBER_RUNNING_QUERIES = "number.running.queries";
    
    /**
     * Never invoked.
     */
    private EnvironmentProperties()
    {
    }
}
