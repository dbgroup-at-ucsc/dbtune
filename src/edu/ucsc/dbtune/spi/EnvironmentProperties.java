package edu.ucsc.dbtune.spi;

import edu.ucsc.dbtune.core.JdbcConnectionManager;

/**
 * Set of properties used to write functional tests. They are used to introspect the system where 
 * the tests are running. The {@code FILE} field indicates where the settings are supposed to be 
 * read from.
 *
 * @author huascar.sanchez@gmail.com (Huascar A. Sanchez)
 */
public class EnvironmentProperties {
    /**
     * Name of the configuration properties file.
     */
    public static final String FILE = "dbtune.cfg";

    /**
     * Developer's DBMS username.
     */
    public static final String USERNAME = JdbcConnectionManager.USERNAME;

    /**
     * Developer's DBMS password
     */
    public static final String PASSWORD = JdbcConnectionManager.PASSWORD;

    /**
     * Name of the target database
     */
    public static final String DATABASE = JdbcConnectionManager.DATABASE;

    /**
     * Fully qualified path to JDBC's {@code Driver} class
     */
    public static final String JDBC_DRIVER = JdbcConnectionManager.DRIVER;

    /**
     * JDBC connection url.
     */
    public static final String URL = JdbcConnectionManager.URL;

    /**
     * Folder for previously defined workloads.
     */
    public static final String WORKLOAD_FOLDER = "workload.dir";

    /**
	 * Name of a previously defined workload.
     */
    public static final String WORKLOAD_NAME = "workload.name";

    /**
	 * Specifies an upper bound on the number of indexes that are monitored by an instance of WFA 
	 * and is used within function {@code chooseCands} (as referenced in page 169 (Figure 6.5) of 
	 * Schnaitter's thesis), and implemented in {@link WorkFunctionAlgorithm#getRecommendation}.
	 *
	 * @see WorkFunctionAlgorithm#getRecommendation
     */
    public static final String MAX_NUM_INDEXES = "max.num.indexes";

    /**
	 * Specifies an upper bound on the number of configurations tracked by WFIT and is read from 
	 * function {@code chooseCands} (as referenced in page 169 (Figure 6.5) of Schnaitter's thesis), 
	 * and implemented in {@link WorkFunctionAlgorithm#getRecommendation}.
	 *
	 * @see WorkFunctionAlgorithm#getRecommendation
     */
    public static final String MAX_NUM_STATES = "max.num.states";

	/**
	 * Whether or not we want to keep the history of the Workload Function Algorithm.
	 *
	 * @see WorkFunctionAlgorithm#WorkFunctionAlgorithm
	 * @see WfaTrace
     */
    public static final String WFA_KEEP_HISTORY = "wfa.keep.history";

	/**
	 * Name of file that contains the set of initial candidates that are loaded into WFIT prior to 
	 * its execution
	 */
    public static final String CANDIDATE_POOL_FILENAME = "candidate.pool.filename";

	/**
	 * Factor used to obtain the overhead of a query based on the start and end time. The overhead 
	 * is obtained by
	 *
	 *   overhead[q] = (startTime - endTime) / OVERHEAD_FACTOR
	 */
    public static final String OVERHEAD_FACTOR = "overhead.factor";

    private EnvironmentProperties(){}
}
