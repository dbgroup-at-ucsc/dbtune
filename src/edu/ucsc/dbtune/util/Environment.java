package edu.ucsc.dbtune.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static edu.ucsc.dbtune.util.EnvironmentProperties.DB2;
import static edu.ucsc.dbtune.util.EnvironmentProperties.FILE;
import static edu.ucsc.dbtune.util.EnvironmentProperties.INDEX_STATISTICS_WINDOW;
import static edu.ucsc.dbtune.util.EnvironmentProperties.JDBC_DRIVER;
import static edu.ucsc.dbtune.util.EnvironmentProperties.JDBC_URL;
import static edu.ucsc.dbtune.util.EnvironmentProperties.MAX_NUM_INDEXES;
import static edu.ucsc.dbtune.util.EnvironmentProperties.MAX_NUM_STATES;
import static edu.ucsc.dbtune.util.EnvironmentProperties.MYSQL;
import static edu.ucsc.dbtune.util.EnvironmentProperties.NUM_PARTITION_ITERATIONS;
import static edu.ucsc.dbtune.util.EnvironmentProperties.OPTIMIZER;
import static edu.ucsc.dbtune.util.EnvironmentProperties.PASSWORD;
import static edu.ucsc.dbtune.util.EnvironmentProperties.PG;
import static edu.ucsc.dbtune.util.EnvironmentProperties.SUPPORTED_OPTIMIZERS;
import static edu.ucsc.dbtune.util.EnvironmentProperties.TEMP_DIR;
import static edu.ucsc.dbtune.util.EnvironmentProperties.USERNAME;
import static edu.ucsc.dbtune.util.EnvironmentProperties.WORKLOADS_FOLDERNAME;

/**
 * @author Huascar A. Sanchez
 * @author Ivo Jimenez
 */
public class Environment
{
    private static Environment instance;

    private final Properties configuration;

    static
    {
        try {
            instance = new Environment();
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    /**
     * Creates an environment object from the given filename.
     *
     * @param propertiesFilename
     *      name of file containing properties
     * @throws IOException
     *      if the file doesn't exist
     */
    public Environment(String propertiesFilename) throws IOException
    {
        configuration = new Properties();
        configuration.load(new FileInputStream(propertiesFilename));
    }

    /**
     * Copy constructor.
     *
     * @param other
     *      object being copied
     */
    public Environment(Environment other)
    {
        configuration = new Properties(other.configuration);
    }

    /**
     * Creates an environment object from the given set of properties.
     *
     * @param properties
     *     properties to be accessed through this object.
     */
    public Environment(Properties properties)
    {
        configuration = new Properties(properties);
    }

    /**
     * Creates an environment object from the default configuration file.
     *
     * @throws IOException
     *      if the default configuration file doesn't exist or can't be read.
     */
    public Environment() throws IOException
    {
        this(FILE);
    }

    /**
     * @return
     *      the environment singleton.
     */
    public static Environment getInstance()
    {
        return instance;
    }
    
    /**
     * @return {@link EnvironmentProperties#TEMP_DIR}
     */
    public String getTempDir()
    {
        return configuration.getProperty(TEMP_DIR);
    }

    /**
     * Assigns a property.
     *
     * @param property
     *     name of the property
     * @param value
     *     value of the property
     */
    public void setProperty(String property, String value)
    {
        configuration.setProperty(property, value);
    }

    /**
     * @return {@link EnvironmentProperties#USERNAME}
     */
    public String getUsername()
    {
        return configuration.getProperty(USERNAME);
    }

    /**
     * @return {@link EnvironmentProperties#PASSWORD}
     */
    public String getPassword()
    {
        return configuration.getProperty(PASSWORD);
    }

    /**
     * @return {@link EnvironmentProperties#JDBC_URL}
     */
    public String getJdbcURL()
    {
        return configuration.getProperty(JDBC_URL);
    }

    /**
     * @return {@link EnvironmentProperties#JDBC_DRIVER}
     */
    public String getJdbcDriver()
    {
        return configuration.getProperty(JDBC_DRIVER);
    }

    /**
     * @return {@link EnvironmentProperties#JDBC_DRIVER}
     */
    public String getVendor()
    {
        return configuration.getProperty(JDBC_DRIVER);
    }

    /**
     * @return {@link EnvironmentProperties#OPTIMIZER}
     * @throws IllegalArgumentException
     *      if the {@link EnvironmentProperties#OPTIMIZER} option doesn't correspond to one 
     *      specified in {@link #SUPPORTED_OPTIMIZERS}
     */
    public String getOptimizer() throws IllegalArgumentException
    {
        String opt = configuration.getProperty(OPTIMIZER);

        if (!SUPPORTED_OPTIMIZERS.contains(opt))
            throw new IllegalArgumentException("Bad optimizer option " + opt);

        return opt;
    }

    /**
     * @return {@link EnvironmentProperties#WORKLOADS_FOLDERNAME}
     */
    public String getWorkloadsFoldername()
    {
        return configuration.getProperty(WORKLOADS_FOLDERNAME);
    }

    /**
     * @return {@link EnvironmentProperties#MAX_NUM_INDEXES}
     */
    public int getMaxNumIndexes()
    {
        String maxSize = configuration.getProperty(MAX_NUM_INDEXES);
        return Integer.valueOf(maxSize);
    }

    /**
     * @return {@link EnvironmentProperties#MAX_NUM_STATES}
     * @throws NumberFormatException
     *      unable to return the max num of states due to the stated reason.
     */
    public int getMaxNumStates() throws NumberFormatException
    {
        String numOfStates = configuration.getProperty(MAX_NUM_STATES);
        return Integer.valueOf(numOfStates);
    }

    /**
     * @return {@link EnvironmentProperties#NUM_PARTITION_ITERATIONS}
     * @throws NumberFormatException
     *      unable to return the overhead factor due to the stated reason.
     */
    public int getNumPartitionIterations() throws NumberFormatException
    {
        String numPartitionIterations = configuration.getProperty(NUM_PARTITION_ITERATIONS);
        return Integer.valueOf(numPartitionIterations);
    }

    /**
     * @return {@link EnvironmentProperties#INDEX_STATISTICS_WINDOW}
     * @throws NumberFormatException
     *      unable to return the overhead factor due to the stated reason.
     */
    public int getIndexStatisticsWindow() throws NumberFormatException
    {
        String indexStatisticsWindow = configuration.getProperty(INDEX_STATISTICS_WINDOW);
        return Integer.valueOf(indexStatisticsWindow);
    }

    /**
     * Returns the path to a given workload file. The path is qualified against the value of {@link 
     * EnvironmentProperties#WORKLOADS_FOLDERNAME}.
     *
     * @param scriptPath
     *    relative path to the file contained in {@link EnvironmentProperties#WORKLOADS_FOLDERNAME}.
     * @return
     *    {@code String} containing the path to the given script filename
     */
    public String getScriptAtWorkloadsFolder(String scriptPath)
    {
        return getWorkloadsFoldername() + scriptPath;
    }

    /**
     * Returns the path to a file inside the workload folder. The path is qualified against the 
     * {@link EnvironmentProperties#WORKLOADS_FOLDERNAME} field. The contents of the returned string 
     * look like:
     * <p>
     * {@code getWorkloadsFoldername()} + "/" + {@code filename}
     *
     * @param filename
     *    name of file contained inside {@link EnvironmentProperties#WORKLOADS_FOLDERNAME}.
     * @return
     *    {@code String} containing the path to the given script filename
     * @see #getWorkloadsFoldername
     */
    public String getFilenameAtWorkloadFolder(String filename)
    {
        return getWorkloadsFoldername() + "/" + filename;
    }

    /**
     * Returns a list where each element corresponds to a workload folder. The path is not 
     * qualified. For example, if {@code #WORKLOADS_FOLDERNAME} contains three workloads, say, 
     * {@code one_table}, {@code movies} and {@code tpch}, the returned list will contain three 
     * elements:
     * <pre>
     * {@code [one_table, movies, tpch]}
     * </pre>
     *
     * @return
     *      list containing the name of each folder for each workload contained inside {@link 
     *      EnvironmentProperties#WORKLOADS_FOLDERNAME}.
     * @see #getWorkloadsFoldername
     */
    public List<String> getWorkloadFolders()
    {
        List<String> workloadFolders = new ArrayList<String>();

        for (File f : (new File(getWorkloadsFoldername())).listFiles())
            if (f.isDirectory())
                workloadFolders.add(f.getAbsolutePath());

        return workloadFolders;
    }

    /**
     * Extracts the driver name by inspecting the {@link EnvironmentProperties#JDBC_URL} property 
     * and assigns it to {@link EnvironmentProperties#JDBC_DRIVER} on the given {@link Environment} 
     * object.
     *
     * @param env
     *     object where the {@link EnvironmentProperties#JDBC_DRIVER} is assigned after it's 
     *     extracted
     * @throws SQLException
     *      if the driver name can't be extracted from the given {@code env} object
     */
    public static void extractDriver(Environment env) throws SQLException
    {
        String driver;

        if (env.getJdbcURL().contains("db2"))
            driver = DB2;
        else if (env.getJdbcURL().contains("mysql"))
            driver = MYSQL;
        else if (env.getJdbcURL().contains("postgres"))
            driver = PG;
        else
            throw new SQLException("Can't extract driver from " + env.getJdbcURL());

        env.setProperty(JDBC_DRIVER, driver);
    }
}
