/* ************************************************************************** *
 *   Copyright 2010 University of California Santa Cruz                       *
 *                                                                            *
 *   Licensed under the Apache License, Version 2.0 (the "License");          *
 *   you may not use this file except in compliance with the License.         *
 *   You may obtain a copy of the License at                                  *
 *                                                                            *
 *       http://www.apache.org/licenses/LICENSE-2.0                           *
 *                                                                            *
 *   Unless required by applicable law or agreed to in writing, software      *
 *   distributed under the License is distributed on an "AS IS" BASIS,        *
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. *
 *   See the License for the specific language governing permissions and      *
 *   limitations under the License.                                           *
 * ************************************************************************** */
package edu.ucsc.dbtune.spi;

import static edu.ucsc.dbtune.spi.EnvironmentProperties.DB2;
import static edu.ucsc.dbtune.spi.EnvironmentProperties.DBMS;
import static edu.ucsc.dbtune.spi.EnvironmentProperties.FILE;
import static edu.ucsc.dbtune.spi.EnvironmentProperties.IBG;
import static edu.ucsc.dbtune.spi.EnvironmentProperties.INDEX_STATISTICS_WINDOW;
import static edu.ucsc.dbtune.spi.EnvironmentProperties.INUM;
import static edu.ucsc.dbtune.spi.EnvironmentProperties.INUM_CACHE_DEPLOYMENT_DIR;
import static edu.ucsc.dbtune.spi.EnvironmentProperties.JDBC_DRIVER;
import static edu.ucsc.dbtune.spi.EnvironmentProperties.JDBC_URL;
import static edu.ucsc.dbtune.spi.EnvironmentProperties.MAX_NUM_INDEXES;
import static edu.ucsc.dbtune.spi.EnvironmentProperties.MAX_NUM_STATES;
import static edu.ucsc.dbtune.spi.EnvironmentProperties.MYSQL;
import static edu.ucsc.dbtune.spi.EnvironmentProperties.NUM_PARTITION_ITERATIONS;
import static edu.ucsc.dbtune.spi.EnvironmentProperties.OPTIMIZER;
import static edu.ucsc.dbtune.spi.EnvironmentProperties.PASSWORD;
import static edu.ucsc.dbtune.spi.EnvironmentProperties.PG;
import static edu.ucsc.dbtune.spi.EnvironmentProperties.USERNAME;
import static edu.ucsc.dbtune.spi.EnvironmentProperties.WORKLOADS_FOLDERNAME;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Map;
import java.util.List;
import java.util.Properties;

/**
 * @author Huascar A. Sanchez
 * @author Ivo Jimenez
 */
public class Environment {

    private final Properties configuration;

    private static final List<String> supportedVendors;
    private static final List<String> supportedOptimizers;

    static {
        supportedVendors = new ArrayList<String>();

        supportedVendors.add(DB2);
        supportedVendors.add(MYSQL);
        supportedVendors.add(PG);

        supportedOptimizers = new ArrayList<String>();

        supportedOptimizers.add(DBMS);
        supportedOptimizers.add(IBG);
        supportedOptimizers.add(INUM);
    }

    /**
     * Creates an environment object from the given filename.
     *
     * @param propertiesFilename
     *     name of file containing properties
     */
    public Environment(String propertiesFilename) throws IOException {
        configuration = new Properties();
        configuration.load(new FileInputStream(propertiesFilename));
    }

    /**
     * Copy constructor
     */
    public Environment(Environment other) {
        configuration = new Properties(other.configuration);
    }

    /**
     * Assigns a property.
     *
     * @param property
     *     name of the property
     * @param value
     *     value of the property
     */
    public void setProperty(String property, String value) {
        configuration.setProperty(property,value);
    }

    /**
     * Creates an environment object from the given set of properties.
     *
     * @param properties
     *     properties to be accessed through this object.
     */
    public Environment(Properties properties) {
        configuration = new Properties(properties);
    }

    /**
     * Creates an environment object from the default configuration file.
     */
    public Environment() throws IOException {
        this(System.getProperty("user.dir") + "/config/" + FILE);
    }

    /**
     * @return {@link EnvironmentProperties#USERNAME}
     */
    public String getUsername() {
        return configuration.getProperty(USERNAME);
    }

    /**
     * @return {@link EnvironmentProperties#PASSWORD}
     */
    public String getPassword() {
        return configuration.getProperty(PASSWORD);
    }

    /**
     * @return {@link EnvironmentProperties#JDBC_URL}
     */
    public String getJdbcURL() {
        return configuration.getProperty(JDBC_URL);
    }

    /**
     * @return {@link EnvironmentProperties#JDBC_DRIVER}
     */
    public String getJdbcDriver() {
        return configuration.getProperty(JDBC_DRIVER);
    }

    /**
     * @return {@link EnvironmentProperties#JDBC_DRIVER}
     */
    public String getVendor() {
        return configuration.getProperty(JDBC_DRIVER);
    }

    /**
     * @return {@link EnvironmentProperties#OPTIMIZER}
     */
    public String getOptimizer() throws IllegalArgumentException {
        String opt = configuration.getProperty(OPTIMIZER);

        if(!getSupportedOptimizers().contains(opt))
            throw new IllegalArgumentException("Bad optimizer option " + opt);

        return opt;
    }

    /**
     * @return {@link EnvironmentProperties#INUM_CACHE_DEPLOYMENT_DIR}
     */
    public String getInumCacheDeploymentDir(){
        return configuration.getProperty(INUM_CACHE_DEPLOYMENT_DIR);
    }

    /**
     * @return {@link EnvironmentProperties#WORKLOADS_FOLDERNAME}
     */
    public String getWorkloadsFoldername(){
        return configuration.getProperty(WORKLOADS_FOLDERNAME);
    }

    /**
     * Returns the list of supported DBMS vendors.
     *
     * @return
     *     list of DBMS vendors that the API supports
     */
    public static List<String> getSupportedVendors()
    {
        return supportedVendors;
    }

    /**
     * Returns the list of supported optimizers.
     *
     * @return
     *     list of optimizers that the API supports
     */
    public static List<String> getSupportedOptimizers()
    {
        return supportedOptimizers;
    }

    /**
     * @return {@link EnvironmentProperties#MAX_NUM_INDEXES}
     */
    public int getMaxNumIndexes(){
        String maxSize = configuration.getProperty(MAX_NUM_INDEXES);
        return Integer.valueOf(maxSize);
    }

    /**
     * @return {@link EnvironmentProperties#MAX_NUM_STATES}
     * @throws NumberFormatException
     *      unable to return the max num of states due to the stated reason.
     */
    public int getMaxNumStates() throws NumberFormatException {
        String numOfStates = configuration.getProperty(MAX_NUM_STATES);
        return Integer.valueOf(numOfStates);
    }

    /**
     * @return {@link EnvironmentProperties#NUM_PARTITION_ITERATIONS}
     * @throws NumberFormatException
     *      unable to return the overhead factor due to the stated reason.
     */
    public int getNumPartitionIterations() throws NumberFormatException {
        String numPartitionIterations = configuration.getProperty(NUM_PARTITION_ITERATIONS);
        return Integer.valueOf(numPartitionIterations);
    }

    /**
     * @return {@link EnvironmentProperties#INDEX_STATISTICS_WINDOW}
     * @throws NumberFormatException
     *      unable to return the overhead factor due to the stated reason.
     */
    public int getIndexStatisticsWindow() throws NumberFormatException {
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
    public String getScriptAtWorkloadsFolder(String scriptPath){
        return getWorkloadsFoldername() + scriptPath;
    }

    /**
     * Returns the path to a file inside the workload folder. The path is qualified against the {@link 
     * EnvironmentProperties#WORKLOADS_FOLDERNAME} field. The contents of the returned string look like:
     * <p>
     * {@code getWorkloadsFoldername()} + "/" + {@code filename}
     *
     * @param filename
     *    name of file contained inside {@link EnvironmentProperties#WORKLOADS_FOLDERNAME}.
     * @return
     *    {@code String} containing the path to the given script filename
     * @see #getWorkloadsFoldername
     */
    public String getFilenameAtWorkloadFolder(String filename){
        return getWorkloadsFoldername() + "/" + filename;
    }

    /**
     * Extracts the driver name by inspecting the {@link EnvironmentProperties#JDBC_URL} property 
     * and assigns it to {@link EnvironmentProperties#JDBC_DRIVER} on the given {@link Environment} 
     * object.
     *
     * @param env
     *     object where the {@link EnvironmentProperties#JDBC_DRIVER} is assigned after it's 
     *     extracted
     */
    public static void extractDriver(Environment env) throws SQLException {
        String driver;

        if(env.getJdbcURL().contains("db2"))
            driver = DB2;
        else if(env.getJdbcURL().contains("mysql"))
            driver = MYSQL;
        else if(env.getJdbcURL().contains("postgres"))
            driver = PG;
        else
            throw new SQLException("Can't extract driver from " + env.getJdbcURL());

        env.setProperty(JDBC_DRIVER, driver);
    }

    static abstract class Configuration {
        private final Properties properties = new Properties();

        /**
         * This method should be overridden to check whether the
         * properties could maybe have changed, and if yes, to reload
         * them.
         */
        protected abstract void checkForPropertyChanges();
        
        protected abstract Properties getDefaults();
        
        protected boolean isDefaultsMode(){ return false; }

        public String getProperty(String propertyName) {
            checkForPropertyChanges();
            synchronized (properties){
                return !isDefaultsMode() ? properties.getProperty(propertyName) : getDefaults().getProperty(propertyName);
            }
        }

        public Properties getAllProperties() {
            synchronized (properties){
                return new Properties(properties);
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
        public final void setProperty(String propertyName, String value) {
            System.out.println("setting " + propertyName + " to " + value);
            synchronized (properties) {
                Object old = properties.get(propertyName);
                if ((value != null && !value.equals(old))
                        || value == null && old != null) {
                System.out.println("setted ");
                    properties.put(propertyName, value);
                }
            }
        }
    }

    static class PropertiesConfiguration extends Configuration {
        private File       file;
        private Properties defaults;
        private long       lastModified = 0;
        private boolean    useDefaults;

        PropertiesConfiguration(Properties defaults) {
            super();
            this.defaults    = defaults;
            this.file        = new File("");
            this.useDefaults = true;

            setAllProperties(this.defaults);
        }

        PropertiesConfiguration(String filename) throws IOException {
            super();

            this.file = new File(filename);

            if(!file.exists()) {
                throw new IOException("File " + filename + " doesn't exist");
            }

            this.defaults    = null;
            this.useDefaults = !file.exists();

            loadPropertiesFromFile();
        }

        @Override
        protected void checkForPropertyChanges() {
            if (lastModified != file.lastModified()) {
                lastModified = file.lastModified();
              try {
                lastModified = file.lastModified();
                loadPropertiesFromFile();
              } catch (IOException e) {
                throw new RuntimeException(e);
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

        private void loadPropertiesFromFile() throws IOException {
            Properties props = new Properties();
            props.load(new FileInputStream(file));
            setAllProperties(props);
        }

        private void setAllProperties(Properties properties) {
            for (Map.Entry<Object, Object> entry : properties.entrySet()) {
                setProperty((String)entry.getKey(), (String) entry.getValue());
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
