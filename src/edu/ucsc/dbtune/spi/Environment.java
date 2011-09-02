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

import edu.ucsc.dbtune.spi.core.Console;

import static edu.ucsc.dbtune.spi.EnvironmentProperties.DBMS;
import static edu.ucsc.dbtune.spi.EnvironmentProperties.FILE;
import static edu.ucsc.dbtune.spi.EnvironmentProperties.IBG;
import static edu.ucsc.dbtune.spi.EnvironmentProperties.INDEX_STATISTICS_WINDOW;
import static edu.ucsc.dbtune.spi.EnvironmentProperties.INUM;
import static edu.ucsc.dbtune.spi.EnvironmentProperties.JDBC_DRIVER;
import static edu.ucsc.dbtune.spi.EnvironmentProperties.MAX_NUM_INDEXES;
import static edu.ucsc.dbtune.spi.EnvironmentProperties.MAX_NUM_STATES;
import static edu.ucsc.dbtune.spi.EnvironmentProperties.NUM_PARTITION_ITERATIONS;
import static edu.ucsc.dbtune.spi.EnvironmentProperties.OPTIMIZER;
import static edu.ucsc.dbtune.spi.EnvironmentProperties.OVERHEAD_FACTOR;
import static edu.ucsc.dbtune.spi.EnvironmentProperties.PASSWORD;
import static edu.ucsc.dbtune.spi.EnvironmentProperties.URL;
import static edu.ucsc.dbtune.spi.EnvironmentProperties.USERNAME;
import static edu.ucsc.dbtune.spi.EnvironmentProperties.WORKLOADS_FOLDERNAME;
import static edu.ucsc.dbtune.util.Objects.cast;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;

/**
 * @author Huascar A. Sanchez
 * @author Ivo Jimenez
 */
public class Environment {

    private final Configuration configuration;

    /**
     * Creates an environment object from the given set of properties.
     *
     * @param properties
     *     properties to be accessed through the this object.
     */
    public Environment(Properties properties) {
        this(new PropertiesConfiguration(properties));
    }

    public Environment() throws IOException {
        this(new PropertiesConfiguration(System.getProperty("user.dir") + "/config/" + FILE));
    }

    Environment(Configuration configuration){
        this.configuration = configuration;
    }


    private static String asString(Object obj){
      return cast(obj, String.class);
    }

    /**
     * @return {@link EnvironmentProperties#URL}
     */
    public String getDatabaseUrl(){
        return asString(configuration.getProperty(URL));
    }

    /**
     * @return {@link EnvironmentProperties#JDBC_DRIVER}
     */
    public String getJDBCDriver(){
        return asString(configuration.getProperty(JDBC_DRIVER));
    }

    /**
     * @return {@link EnvironmentProperties#USERNAME}
     */
    public String getUsername(){
        return asString(configuration.getProperty(USERNAME));
    }

    /**
     * @return {@link EnvironmentProperties#OPTIMIZER}
     */
    public String getOptimizer() throws IllegalArgumentException {
        String opt = asString(configuration.getProperty(OPTIMIZER));

        if(!opt.equals(INUM) && !opt.equals(IBG) && !opt.equals(DBMS)) {
            throw new IllegalArgumentException("Bad optimizer option " + opt);
        }

        return opt;
    }

    /**
     * @return {}EnvironmentProperties#PASSWORD
     */
    public String getPassword(){
        return asString(configuration.getProperty(PASSWORD));
    }

    /**
     * @return {@link EnvironmentProperties#WORKLOADS_FOLDERNAME}
     */
    public String getWorkloadsFoldername(){
        return asString(configuration.getProperty(WORKLOADS_FOLDERNAME));
    }

    /**
     * @return {@link EnvironmentProperties#MAX_NUM_INDEXES}
     */
    public int getMaxNumIndexes(){
        String maxSize = asString(configuration.getProperty(MAX_NUM_INDEXES));
        return Integer.valueOf(maxSize);
    }

    /**
     * @return {@link EnvironmentProperties#MAX_NUM_STATES}
     * @throws NumberFormatException
     *      unable to return the max num of states due to the stated reason.
     */
    public int getMaxNumStates() throws NumberFormatException {
        String numOfStates = asString(configuration.getProperty(MAX_NUM_STATES));
        return Integer.valueOf(numOfStates);
    }

    /**
     * @return {@link EnvironmentProperties#OVERHEAD_FACTOR}
     * @throws NumberFormatException
     *      unable to return the overhead factor due to the stated reason.
     */
    public float getOverheadFactor() throws NumberFormatException {
        String overheadFactor = asString(configuration.getProperty(OVERHEAD_FACTOR));
        return Float.valueOf(overheadFactor);
    }

    /**
     * @return {@link EnvironmentProperties#NUM_PARTITION_ITERATIONS}
     * @throws NumberFormatException
     *      unable to return the overhead factor due to the stated reason.
     */
    public int getNumPartitionIterations() throws NumberFormatException {
        String numPartitionIterations = asString(configuration.getProperty(NUM_PARTITION_ITERATIONS));
        return Integer.valueOf(numPartitionIterations);
    }

    /**
     * @return {@link EnvironmentProperties#INDEX_STATISTICS_WINDOW}
     * @throws NumberFormatException
     *      unable to return the overhead factor due to the stated reason.
     */
    public int getIndexStatisticsWindow() throws NumberFormatException {
        String indexStatisticsWindow = asString(configuration.getProperty(INDEX_STATISTICS_WINDOW));
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
                Console.streaming().error("unable to load properties" + e);
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
                setProperty(asString(entry.getKey()), entry.getValue());
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
