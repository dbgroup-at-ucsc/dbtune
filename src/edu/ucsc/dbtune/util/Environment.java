package edu.ucsc.dbtune.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Properties;

import static com.google.common.base.Strings.isNullOrEmpty;

import static edu.ucsc.dbtune.util.EnvironmentProperties.CANDIDATE_GENERATOR;
import static edu.ucsc.dbtune.util.EnvironmentProperties.DB2;
import static edu.ucsc.dbtune.util.EnvironmentProperties.FILE;
import static edu.ucsc.dbtune.util.EnvironmentProperties.INUM_MATCHING_STRATEGY;
import static edu.ucsc.dbtune.util.EnvironmentProperties.INUM_SLOT_CACHE;
import static edu.ucsc.dbtune.util.EnvironmentProperties.INUM_SPACE_COMPUTATION;
import static edu.ucsc.dbtune.util.EnvironmentProperties.JDBC_DRIVER;
import static edu.ucsc.dbtune.util.EnvironmentProperties.JDBC_URL;
import static edu.ucsc.dbtune.util.EnvironmentProperties.MYSQL;
import static edu.ucsc.dbtune.util.EnvironmentProperties.OPTIMIZER;
import static edu.ucsc.dbtune.util.EnvironmentProperties.PASSWORD;
import static edu.ucsc.dbtune.util.EnvironmentProperties.PG;
import static edu.ucsc.dbtune.util.EnvironmentProperties.SPACE_BUDGET;
import static edu.ucsc.dbtune.util.EnvironmentProperties.SUPPORTED_OPTIMIZERS;
import static edu.ucsc.dbtune.util.EnvironmentProperties.TEMP_DIR;
import static edu.ucsc.dbtune.util.EnvironmentProperties.USERNAME;
import static edu.ucsc.dbtune.util.EnvironmentProperties.WFIT_INDEX_STATISTICS_WINDOW;
import static edu.ucsc.dbtune.util.EnvironmentProperties.WFIT_MAX_NUM_INDEXES;
import static edu.ucsc.dbtune.util.EnvironmentProperties.WFIT_MAX_NUM_STATES;
import static edu.ucsc.dbtune.util.EnvironmentProperties.WFIT_NUM_PARTITION_ITERATIONS;
import static edu.ucsc.dbtune.util.EnvironmentProperties.WORKLOADS_FOLDERNAME;
import static edu.ucsc.dbtune.util.Strings.toBoolean;

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
     * Checks if a property is null or empty and throws a {@link NoSuchMethodException} if it is.
     *
     * @param properties
     *      used to get the corresponding name
     * @param propertyName
     *      property being checked
     * @return
     *      the corresponding property
     * @throws NoSuchElementException
     *      if the property is empty or null
     */
    private static String getOrThrowIfNullOrEmpty(Properties properties, String propertyName)
        throws NoSuchElementException
    {
        String propertyValue = properties.getProperty(propertyName);
        
        if (isNullOrEmpty(propertyValue))
            throw new NoSuchElementException(propertyName + " not specified or empty");

        return propertyValue;
    }
    
    /**
     * @return {@link EnvironmentProperties#TEMP_DIR}
     * @throws NoSuchElementException
     *      if the property is empty or null
     */
    public String getTempDir()
    {
        return getOrThrowIfNullOrEmpty(configuration, TEMP_DIR);
    }

    /**
     * @return {@link EnvironmentProperties#USERNAME}
     * @throws NoSuchElementException
     *      if the property is empty or null
     */
    public String getUsername()
    {
        return getOrThrowIfNullOrEmpty(configuration, USERNAME);
    }

    /**
     * @return {@link EnvironmentProperties#PASSWORD}
     * @throws NoSuchElementException
     *      if the property is empty or null
     */
    public String getPassword()
    {
        return getOrThrowIfNullOrEmpty(configuration, PASSWORD);
    }

    /**
     * @return {@link EnvironmentProperties#JDBC_URL}
     * @throws NoSuchElementException
     *      if the property is empty or null
     */
    public String getJdbcURL()
    {
        return getOrThrowIfNullOrEmpty(configuration, JDBC_URL);
    }

    /**
     * @return {@link EnvironmentProperties#JDBC_DRIVER}
     * @throws NoSuchElementException
     *      if the property is empty or null
     */
    public String getJdbcDriver()
    {
        return getOrThrowIfNullOrEmpty(configuration, JDBC_DRIVER);
    }

    /**
     * @return {@link EnvironmentProperties#JDBC_DRIVER}
     * @throws NoSuchElementException
     *      if the property is empty or null
     */
    public String getVendor()
    {
        return getOrThrowIfNullOrEmpty(configuration, JDBC_DRIVER);
    }

    /**
     * @return {@link EnvironmentProperties#OPTIMIZER}
     * @throws IllegalArgumentException
     *      if the {@link EnvironmentProperties#OPTIMIZER} option doesn't correspond to one 
     *      specified in {@link #SUPPORTED_OPTIMIZERS}
     * @throws NoSuchElementException
     *      if the property is empty or null
     */
    public String getOptimizer() throws IllegalArgumentException
    {
        String opt = getOrThrowIfNullOrEmpty(configuration, OPTIMIZER);

        if (!SUPPORTED_OPTIMIZERS.contains(opt))
            throw new IllegalArgumentException("Bad optimizer option " + opt);

        return opt;
    }

    /**
     * @return {@link EnvironmentProperties#WORKLOADS_FOLDERNAME}
     * @throws NoSuchElementException
     *      if the property is empty or null
     */
    public String getWorkloadsFoldername()
    {
        return getOrThrowIfNullOrEmpty(configuration, WORKLOADS_FOLDERNAME);
    }

    /**
     * @return {@link EnvironmentProperties#SPACE_BUDGET}
     * @throws NoSuchElementException
     *      if the property is empty or null
     */
    public int getSpaceBudget()
    {
        String budget = getOrThrowIfNullOrEmpty(configuration, SPACE_BUDGET);
        return Integer.valueOf(budget);
    }

    /**
     * @return {@link EnvironmentProperties#CANDIDATE_GENERATOR}
     * @throws NoSuchElementException
     *      if the property is empty or null
     */
    public List<String> getCandidateGenerator()
    {
        return Arrays.asList(
                getOrThrowIfNullOrEmpty(configuration, CANDIDATE_GENERATOR).split(","));
    }

    /**
     * @return {@link EnvironmentProperties#MAX_NUM_INDEXES}
     * @throws NoSuchElementException
     *      if the property is empty or null
     */
    public int getMaxNumIndexes()
    {
        String maxSize = getOrThrowIfNullOrEmpty(configuration, WFIT_MAX_NUM_INDEXES);
        return Integer.valueOf(maxSize);
    }

    /**
     * @return {@link EnvironmentProperties#MAX_NUM_STATES}
     * @throws NumberFormatException
     *      unable to return the max num of states due to the stated reason.
     * @throws NoSuchElementException
     *      if the property is empty or null
     */
    public int getMaxNumStates() throws NumberFormatException
    {
        String numOfStates = getOrThrowIfNullOrEmpty(configuration, WFIT_MAX_NUM_STATES);
        return Integer.valueOf(numOfStates);
    }

    /**
     * @return {@link EnvironmentProperties#NUM_PARTITION_ITERATIONS}
     * @throws NumberFormatException
     *      unable to return the overhead factor due to the stated reason.
     * @throws NoSuchElementException
     *      if the property is empty or null
     */
    public int getNumPartitionIterations() throws NumberFormatException
    {
        return Integer.valueOf(
                getOrThrowIfNullOrEmpty(configuration, WFIT_NUM_PARTITION_ITERATIONS));
    }

    /**
     * @return {@link EnvironmentProperties#INDEX_STATISTICS_WINDOW}
     * @throws NumberFormatException
     *      unable to return the overhead factor due to the stated reason.
     * @throws NoSuchElementException
     *      if the property is empty or null
     */
    public int getIndexStatisticsWindow() throws NumberFormatException
    {
        return Integer.valueOf(
                getOrThrowIfNullOrEmpty(configuration, WFIT_INDEX_STATISTICS_WINDOW));
    }

    /**
     * Returns the path to a given workload file. The path is qualified against the value of {@link 
     * EnvironmentProperties#WORKLOADS_FOLDERNAME}.
     *
     * @param scriptPath
     *    relative path to the file contained in {@link EnvironmentProperties#WORKLOADS_FOLDERNAME}.
     * @return
     *    {@code String} containing the path to the given script filename
     * @throws NoSuchElementException
     *      if the property is empty or null
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
     * @throws NoSuchElementException
     *      if the property is empty or null
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
     * @throws NoSuchElementException
     *      if the property is empty or null
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

    /**
     * @return {@link EnvironmentProperties#INUM_SPACE_COMPUTATION}
     * @throws NoSuchElementException
     *      if the property is empty or null
     */
    public String getInumSpaceComputation()
    {
        return getOrThrowIfNullOrEmpty(configuration, INUM_SPACE_COMPUTATION);
    }

    /**
     * @return {@link EnvironmentProperties#INUM_MATCHING_STRATEGY}
     * @throws NoSuchElementException
     *      if the property is empty or null
     */
    public String getInumMatchingStrategy()
    {
        return getOrThrowIfNullOrEmpty(configuration, INUM_MATCHING_STRATEGY);
    }

    /**
     * @return {@link EnvironmentProperties#INUM_SLOT_CACHE}
     * @throws NoSuchElementException
     *      if the property is empty or null
     */
    public boolean getInumSlotCache()
    {
        return toBoolean(getOrThrowIfNullOrEmpty(configuration, INUM_SLOT_CACHE));
    }
}
