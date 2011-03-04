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

    private EnvironmentProperties(){}
}
