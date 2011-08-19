package edu.ucsc.dbtune.spi;

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
    public static final String FILE = "dbtune.cfg"; /**
     * Developer's DBMS username.
     */
    public static final String USERNAME = "username";

    /**
     * Developer's DBMS password
     */
    public static final String PASSWORD = "password";

    /**
     * Name of the target database
     */
    public static final String DATABASE = "database";

    /**
     * Name of the target schema (for postgres)
     */
    public static final String SCHEMA = "schema";

    /**
     * Fully qualified path to JDBC's {@code Driver} class
     */
    public static final String JDBC_DRIVER = "driver";

    /**
     * JDBC connection url.
     */
    public static final String URL = "url";

    /**
     * Type of optimizer to use
     */
    public static final String OPTIMIZER = "optimizer";

    /**
     * DBMS optimizer
     */
    public static final String DBMS = "dbms";

    /**
     * INUM optimizer
     */
    public static final String INUM = "inum";

    /**
     * IBG optimizer
     */
    public static final String IBG = "ibg";

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
     * Specifies an upper bound on the number of indexes that are monitored by an instance of WFA 
     * and is used within function {@code chooseCands} (as referenced in page 169 (Figure 6.5) of 
     * Schnaitter's thesis), and implemented in {@link 
     * edu.ucsc.dbtune.advisor.wfit.WorkFunctionAlgorithm#getRecommendation}.
     *
     * @see edu.ucsc.dbtune.advisor.wfit.WorkFunctionAlgorithm#getRecommendation
     */
    public static final String MAX_NUM_INDEXES = "max.number.of.indexes";

    /**
     * Specifies an upper bound on the number of configurations tracked by WFIT and is read from 
     * function {@code chooseCands} (as referenced in page 169 (Figure 6.5) of Schnaitter's thesis), 
     * and implemented in {@link 
     * edu.ucsc.dbtune.advisor.wfit.WorkFunctionAlgorithm#getRecommendation}.
     *
     * @see edu.ucsc.dbtune.advisor.wfit.WorkFunctionAlgorithm#getRecommendation
     */
    public static final String MAX_NUM_STATES = "max.number.of.states";

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

    private EnvironmentProperties(){}
}
