package edu.ucsc.dbtune.spi;

import edu.ucsc.dbtune.util.Objects;
import edu.ucsc.dbtune.spi.core.Console;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.lang.NumberFormatException;

import static edu.ucsc.dbtune.spi.EnvironmentProperties.FILE;
import static edu.ucsc.dbtune.spi.EnvironmentProperties.URL;
import static edu.ucsc.dbtune.spi.EnvironmentProperties.JDBC_DRIVER;
import static edu.ucsc.dbtune.spi.EnvironmentProperties.DATABASE;
import static edu.ucsc.dbtune.spi.EnvironmentProperties.USERNAME;
import static edu.ucsc.dbtune.spi.EnvironmentProperties.PASSWORD;
import static edu.ucsc.dbtune.spi.EnvironmentProperties.WORKLOADS_FOLDERNAME;
import static edu.ucsc.dbtune.spi.EnvironmentProperties.WORKLOAD_NAME;
import static edu.ucsc.dbtune.spi.EnvironmentProperties.MAX_NUM_INDEXES;
import static edu.ucsc.dbtune.spi.EnvironmentProperties.MAX_NUM_STATES;
import static edu.ucsc.dbtune.spi.EnvironmentProperties.WFA_KEEP_HISTORY;
import static edu.ucsc.dbtune.spi.EnvironmentProperties.OVERHEAD_FACTOR;
import static edu.ucsc.dbtune.spi.EnvironmentProperties.CANDIDATE_POOL_FILENAME;
import static edu.ucsc.dbtune.spi.EnvironmentProperties.QUERY_PROFILE_FILENAME;
import static edu.ucsc.dbtune.spi.EnvironmentProperties.WFIT_LOG_FILENAME;
import static edu.ucsc.dbtune.spi.EnvironmentProperties.OPT_LOG_FILENAME;
import static edu.ucsc.dbtune.spi.EnvironmentProperties.MIN_WF_FILENAME;
import static edu.ucsc.dbtune.util.Objects.as;

/**
 * @author huascar.sanchez@gmail.com (Huascar A. Sanchez)
 */
public class Environment {
    private final Configuration configuration;

    public Environment() throws IOException {
        this(
                new PropertiesConfiguration(
                        getDefaultProperties(),
                        System.getProperty("user.dir") + "/config/" + FILE
                )
        );
    }

    Environment(Configuration configuration){
        this.configuration = configuration;
    }

    /**
     * @see EnvironmentProperties#URL
     */
    public String getDatabaseUrl(){
        return as(configuration.getProperty(URL));
    }

    /**
     * @see EnvironmentProperties#JDBC_DRIVER
     */
    public String getJDBCDriver(){
        return as(configuration.getProperty(JDBC_DRIVER));
    }

    /**
     * @see EnvironmentProperties#DATABASE
     */
    public String getDatabaseName(){
        return as(configuration.getProperty(DATABASE));
    }

    /**
     * @see EnvironmentProperties#USERNAME
     */
    public String getUsername(){
        return as(configuration.getProperty(USERNAME));
    }

    /**
     * @see EnvironmentProperties#PASSWORD
     */
    public String getPassword(){
        return as(configuration.getProperty(PASSWORD));
    }

    /**
     * @see EnvironmentProperties#WORKLOADS_FOLDERNAME
     */
    public String getWorkloadsFoldername(){
        return as(configuration.getProperty(WORKLOADS_FOLDERNAME));
    }

    /**
     * @see EnvironmentProperties#WORKLOAD_NAME
     */
    public String getWorkloadName(){
        return as(configuration.getProperty(WORKLOAD_NAME));
    }

    /**
     * @see EnvironmentProperties#MAX_NUM_INDEXES
     */
    public int getMaxNumIndexes(){
        String maxSize = (String) as(configuration.getProperty(MAX_NUM_INDEXES));
        return Integer.valueOf(maxSize);
    }

    /**
     * @see EnvironmentProperties#WFA_KEEP_HISTORY
     */
    public boolean getWFAKeepHistory(){
        String keepHistory = (String) as(configuration.getProperty(WFA_KEEP_HISTORY));
        return Boolean.valueOf(keepHistory);
    }

    /**
     * @see EnvironmentProperties#MAX_NUM_STATES
     */
    public int getMaxNumStates() throws NumberFormatException {
        String numOfStates = (String) as(configuration.getProperty(MAX_NUM_STATES));
        return Integer.valueOf(numOfStates);
    }

    /**
     * @see EnvironmentProperties#OVERHEAD_FACTOR
     */
    public float getOverheadFactor() throws NumberFormatException {
        String overheadFactor = (String) as(configuration.getProperty(OVERHEAD_FACTOR));
        return Float.valueOf(overheadFactor);
    }

    /**
     * @see EnvironmentProperties#CANDIDATE_POOL_FILENAME
     */
    public String getCandidatePoolFilename(){
        return as(configuration.getProperty(CANDIDATE_POOL_FILENAME));
    }

    /**
     * @see EnvironmentProperties#QUERY_PROFILE_FILENAME
     */
    public String getQueryProfileFilename(){
        return as(configuration.getProperty(QUERY_PROFILE_FILENAME));
    }

    /**
     * @see EnvironmentProperties#WFIT_LOG_FILENAME
     */
    public String getWFITLogFilename(){
        return as(configuration.getProperty(WFIT_LOG_FILENAME));
    }

    /**
     * @see EnvironmentProperties#OPT_LOG_FILENAME
     */
    public String getOPTLogFilename(){
        return as(configuration.getProperty(OPT_LOG_FILENAME));
    }

    /**
     * @see EnvironmentProperties#MIN_WF_FILENAME
     */
    public String getMinWFFilename(){
        return as(configuration.getProperty(MIN_WF_FILENAME));
    }

    /**
     * Returns the path to a given workload file. The path is qualified against the value of {@link 
     * EnvironmentProperties#WORKLOAD_FOLDERNAME}.
     *
     * @param scriptPath
     *    relative path to the file contained in {@link EnvironmentProperties#WORKLOAD_FOLDERNAME}.
     * @return
     *    {@code String} containing the path to the given script filename
     */
    public String getScriptAtWorkloadsFolder(String scriptPath){
        return getWorkloadsFoldername() + scriptPath;
    }

    /**
     * Returns the path to a file inside the workload folder. The path is qualified against the 
     * concatenation of {@link WORKLOADS_FOLDERNAME} and {@link WORKLOAD_NAME}. The contents of the 
     * returned string look like:
     * <p>
     * {@link getWorkloadsFoldername()} + "/" + {@linkgetWorkloadName() + {@code filename} }
     *
     * @param filename
     *    name of file contained inside {@link EnvironmentProperties#WORKLOAD_FOLDERNAME}.
     * @return
     *    {@code String} containing the path to the given script filename
     * @see getWorkloadsFoldername
     * @see getWorkloadName
     */
    public String getFilenameAtWorkloadFolder(String filename){
        return getWorkloadsFoldername() + "/" + getWorkloadName() + "/" + filename;
    }

    /**
     * Returns all the properties defined in the {@code Environment} class as a {@link Properties} 
     * object.
     * <p>
     * Note that the {@code Properties} object returned by this method, unlike the other getters of 
     * the class, is not thread safe.
     *
     * @return {@code Properties} object containing all the settings contained in the class
     */
    public Properties getAll(){
        Properties properties = new Properties();

        for ( Entry<String, Object> property : configuration.getAllProperties() ) {
            properties.setProperty( property.getKey(), property.getValue().toString() );
        }

        return properties;
    }

    private static Properties getDefaultProperties(){
        return new Properties(){
            {
                setProperty(URL,         "jdbc:postgresql://aigaion.cse.ucsc.edu/test");
                setProperty(USERNAME,    "dbtune");
                setProperty(PASSWORD,    "dbtuneadmin");
                setProperty(WORKLOADS_FOLDERNAME, "resources/test/postgres");
                setProperty(DATABASE,    "test");
                setProperty(JDBC_DRIVER, "org.postgresql.Driver");
            }
        };
    }

    static interface Configuration {
       Object getProperty(String propertyName);
       Set<Entry<String,Object>> getAllProperties();
    }

    static abstract class AbstractConfiguration implements Configuration {
        private final Map<String, Object> properties = new HashMap<String, Object>();

        AbstractConfiguration(){}

        /**
         * This method should be overridden to check whether the
         * properties could maybe have changed, and if yes, to reload
         * them.
         */
        protected abstract void checkForPropertyChanges();
        
        protected abstract Properties getDefaults();
        
        protected boolean isDefaultsMode(){ return false; }

        @Override
        public Object getProperty(String propertyName) {
            checkForPropertyChanges();
            synchronized (properties){
                return !isDefaultsMode() ? properties.get(propertyName) : getDefaults().getProperty(propertyName);
            }
        }

        @Override
        public Set<Entry<String, Object>> getAllProperties() {
            synchronized (properties){
                return properties.entrySet();
            }
        }

        /**
       * setting a property.
       *
       * @param propertyName
       *     name of property
       * @param value
       *    value of property.
       */
      protected final void setProperty(String propertyName, Object value) {
        synchronized (properties) {
          Object old = properties.get(propertyName);
          if ((value != null && !value.equals(old))
              || value == null && old != null) {
            properties.put(propertyName, value);
          }
        }
      }
    }

    static class PropertiesConfiguration extends AbstractConfiguration {
        private final File          file;
        private final Properties    defaults;

        private boolean useDefaults = false;
        private long    lastModified = 0;

        PropertiesConfiguration(Properties defaults, String filename) throws IOException {
            super();
            this.defaults       = defaults;
            this.file           = new File(filename);
            this.useDefaults    = !file.exists();
            loadProperties();
        }

        @Override
        protected void checkForPropertyChanges() {
            if (lastModified != file.lastModified()) {
              try {
                lastModified = file.lastModified();
                loadProperties();
              } catch (IOException e) {
                Console.streaming().info("Error: unable to load properties", e);
              }
            }
        }

        @Override
        protected Properties getDefaults() {
            return defaults;
        }

        @Override
        protected boolean isDefaultsMode() {
            return useDefaults;
        }

        private void loadProperties() throws IOException {
            final Properties properties = new Properties();
            if(!useDefaults){
                properties.load(new FileInputStream(file));
                setAllProperties(properties);
            } else {
                setAllProperties(getDefaultProperties());
            }
        }

        private void setAllProperties(Properties properties) {
            for (Map.Entry<Object, Object> entry : properties.entrySet()) {
                setProperty(Objects.<String>as(entry.getKey()), entry.getValue());
            }
        }
    }


    /**
     * @return the environment singleton.
     */
    public static Environment getInstance(){
        return Installer.INSTANCE;
    }

    /** Lazy-constructed singleton, which is thread safe */
    static class Installer {
        static final Environment INSTANCE;

        static {
            try {
                INSTANCE = new Environment();
            } catch (IOException e) {
                throw new IllegalStateException(e);
            }
        }
    }
}
