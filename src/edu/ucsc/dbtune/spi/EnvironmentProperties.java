package edu.ucsc.dbtune.spi;

/**
 * @author huascar.sanchez@gmail.com (Huascar A. Sanchez)
 */
public class EnvironmentProperties {
    /**
     * The name of the configuration properties file.
     */
    public static final String FILE = "dbtune.cfg";

    /**
     * Name of the property giving the developer's user-name.
     */
    public static final String USER_NAME = "test.dbms.username";

    /**
     * Name of the property giving the developer's password..
     */
    public static final String PASSWORD = "test.dbms.password";

    /**
     * Name of the property used by functional tests to load postgres's prepared
     * workloads.
     */
    public static final String SCRIPT_DIRECTORY = "test.dbms.script.dir";

    /**
     * Name of the property given the name of the database..
     */
    public static final String DATABASE = "test.dbms.database";

    /**
     * Name of the property use by functional tests to create a JDBC driver.
     */
    public static final String JDBC_DRIVER = "test.dbms.driver";

    /**
     * Name of property given connection url.
     */
    public static final String URL = "test.dbms.url";

    private EnvironmentProperties(){}
}
