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

/**
 * Set of properties used in the DBTune API. They are used to introspect the environment where the 
 * API is running. The {@code FILE} field indicates where the settings are supposed to be read from.
 *
 * @author Huascar A. Sanchez
 * @author Ivo Jimenez
 */
public class EnvironmentProperties
{
    // XXX: Note to devs.
    //
    // Each set of properties is logically grouped (using 3 empty lines to separate groups; no space 
    // between properties from the same group) based on what they refer to, eg. connectivity, 
    // supported dbms, optimizers available, etc..
    // 
    // For some groups, the addition of a new property may need to be reflected in other classes of 
    // the API. When this is the case, there will be a note indicating the list of class#method to 
    // update.

    /**
     * Never invoked
     */
    private EnvironmentProperties(){}



    /**
     * Name of the configuration properties file.
     */
    public static final String FILE = "dbtune.cfg";



    /**
     * Developer's DBMS username.
     */
    public static final String USERNAME = "username";
    /**
     * Developer's DBMS password
     */
    public static final String PASSWORD = "password";
    /**
     * JDBC connection url.
     */
    public static final String URL = "url";



    /**
     * Fully qualified path to JDBC's {@code Driver} class
     */
    public static final String JDBC_DRIVER = "driver";
    /**
     * DB2 driver class path
     */
    public static final String DB2 = "com.ibm.db2.jcc.DB2Driver";
    /**
     * MySQL driver class path
     */
    public static final String MYSQL = "com.mysql.jdbc.Driver";
    /**
     * PG driver class path
     */
    public static final String PG = "org.postgresql.Driver";



    // XXX: Note to devs
    // 
    // if a new type of optimizer is added, the DatabaseSystem.getAvailableOptimizers() method 
    // should be updated accordingly.
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
    *  Folder for storing inum cache files and/or workload files. By convention this folder won't
    *  contain any sub-folders.
    */
    public static final String INUM_CACHE_DEPLOYMENT_DIR = "cache.deployment.dir";

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
     * XXX document
     */
    public static final String NUM_PARTITION_ITERATIONS = "num.partition.iterations";
    /**
     * XXX document
     */
    public static final String INDEX_STATISTICS_WINDOW = "index.statistics.window";
}
