package edu.ucsc.dbtune.spi;

import edu.ucsc.dbtune.advisor.WorkFunctionAlgorithm;
import edu.ucsc.dbtune.connectivity.JdbcConnectionManager;

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
     * Folder for storing output files. By convention, this folder contains many sub-folders where 
     * each one corresponds to one workload. The contents of each workload folder depend on what 
     * test has been run.
     */
    public static final String OUTPUT_FOLDERNAME = "output.dir";

    /**
     * Name of a previously defined workload. This is used to uniquely identify a workload inside 
     * the {@code WORKLOADS_FOLDERNAME}.
     *
     * @see #WORKLOADS_FOLDERNAME
     */
    public static final String WORKLOAD_NAME = "workload.name";

    /**
     * Specifies an upper bound on the number of indexes that are monitored by an instance of WFA 
     * and is used within function {@code chooseCands} (as referenced in page 169 (Figure 6.5) of 
     * Schnaitter's thesis), and implemented in {@link WorkFunctionAlgorithm#getRecommendation()}.
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
     * @see WorkFunctionAlgorithm
     * @see edu.ucsc.dbtune.advisor.WfaTrace
     */
    public static final String WFA_KEEP_HISTORY = "wfa.keep.history";

    /**
     * Factor used to obtain the overhead of a query based on the start and end time. The overhead 
     * is obtained by
     *
     *   overhead[q] = (startTime - endTime) / OVERHEAD_FACTOR
     */
    public static final String OVERHEAD_FACTOR = "overhead.factor";

    /**
     * XXX document
     */
    public static final String NUM_PARTITION_ITERATIONS = "num.partition.iterations";

    /**
     * XXX document
     */
    public static final String INDEX_STATISTICS_WINDOW = "index.statistics.window";

    /**
     * XXX document it
     */
    public static final String WFIT_LOG_FILENAME = "wfit.log.filename";

    /**
     * XXX document it
     */
    public static final String OPT_LOG_FILENAME = "opt.log.filename";

    /**
     * XXX document it
     */
    public static final String MIN_WF_FILENAME = "min.wf.filename";

    private EnvironmentProperties(){}
}
