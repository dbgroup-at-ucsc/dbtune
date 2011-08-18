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
package edu.ucsc.dbtune;

import edu.ucsc.dbtune.metadata.Catalog;
import edu.ucsc.dbtune.metadata.Column;
import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.metadata.extraction.MetadataExtractor;
import edu.ucsc.dbtune.metadata.extraction.MySQLExtractor;
import edu.ucsc.dbtune.metadata.extraction.PGExtractor;
import edu.ucsc.dbtune.optimizer.Optimizer;
import edu.ucsc.dbtune.optimizer.DB2Optimizer;
import edu.ucsc.dbtune.optimizer.IBGOptimizer;
import edu.ucsc.dbtune.optimizer.PGOptimizer;
import edu.ucsc.dbtune.spi.Environment;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.DriverManager;
import java.util.List;

/**
 * Represents a DBMS system. There's a single connection used to communicate with the system. The 
 * user is responsible for closing it.
 *
 * @author Huascar A. Sanchez
 * @author Ivo Jimenez
 */
public class DatabaseSystem
{
    private Environment environment;
    private Connection  connection;
    private Optimizer   optimizer;
    private Catalog     catalog;

    /**
     * Creates a database system instance with the given properties.
     *
     * @param environment
     *     an environment object used to access the properties of the system
     */
    public DatabaseSystem(Environment environment) throws SQLException
    {
        this.environment = environment;

        try {
            Class.forName(environment.getJDBCDriver());
        } catch(Exception e) {
            throw new SQLException(e);
        }

        connection = connect();
        catalog    = getCatalog(connection);
        optimizer  = getOptimizer(connection,catalog);
    }

    /**
     * Creates a database system instance with the properties from {@link Environment}.
     *
     * @param properties
     *     settings to be used in the construction of the object
     * @see Environment
     * @see EnvironmentProperties
     */
    public DatabaseSystem() throws SQLException
    {
        this(Environment.getInstance());
    }

    /**
     * Returns the corresponding optimizer.
     *
     * @return
     *      an optimizer.
     * @see Optimizer
     */
    private Optimizer getOptimizer(Connection connection, Catalog catalog) throws SQLException
    {
        if(environment.getJDBCDriver().equals("com.mysql.jdbc.Driver")) {
            //optimizer = new MySQLOptimizer(connection, catalog);
            throw new SQLException("MySQLOptimizer doesn't exist yet");
        } else if(environment.getJDBCDriver().equals("com.ibm.db2.jcc.DB2Driver")) {
            //optimizer = new DB2Optimizer(connection, catalog);
            optimizer = new DB2Optimizer(connection, environment.getDatabaseName());
        } else if(environment.getJDBCDriver().equals("org.postgresql.Driver")) {
            //optimizer = new PGOptimizer(connection, catalog.findSchema(environment.getSchema()));
            optimizer = new PGOptimizer(connection);
        } else {
            throw new SQLException("Unsupported driver " + environment.getJDBCDriver());
        }

        if(environment.getOptimizer().equals("dbms")) {
            return optimizer;
        } else if(environment.getOptimizer().equals("ibg")) {
            return new IBGOptimizer(optimizer);
        } else if(environment.getOptimizer().equals("inum")) {
            throw new SQLException("INUMOptimizer doesn't exist yet");
        } else {
            throw new SQLException("unknown optimizer option: " + environment.getOptimizer());
        }
    }

    /**
     * Returns the corresponding metadata extractor.
     *
     * @return
     *      a metadata extractor
     */
    private Catalog getCatalog(Connection connection) throws SQLException
    {
        MetadataExtractor extractor = null;

        if(environment.getJDBCDriver().equals("com.mysql.jdbc.Driver")) {
            extractor = new MySQLExtractor();
        } else if(environment.getJDBCDriver().equals("com.ibm.db2.jcc.DB2Driver")) {
            throw new SQLException("DB2Extractor doesn't exist yet");
        } else if(environment.getJDBCDriver().equals("org.postgresql.Driver")) {
            extractor = new PGExtractor();
        } else {
            throw new SQLException("Unsupported driver " + environment.getJDBCDriver());
        }

        return extractor.extract(connection);
    }

    /**
     * Returns the corresponding optimizer.
     *
     * @return
     *      an optimizer.
     * @see Optimizer
     */
    public Optimizer getOptimizer()
    {
        return optimizer;
    }

    /**
     * Returns the corresponding catalog
     *
     * @return
     *      a metadata extractor
     */
    public Catalog getCatalog()
    {
        return catalog;
    }

    /**
     * Creates and opens a connection to the DBMS. The user is responsible for closing it.
     *
     * @return
     *      a connection
     * @see Connection
     */
    private Connection connect() throws SQLException {
        String url = environment.getDatabaseUrl();
        String usr = environment.getUsername();
        String pwd = environment.getPassword();
        
        return DriverManager.getConnection(url,usr,pwd);

        // may need to run a SET SEARCH_PATH for postgres in order to set the schema name correctly
    }

    /**
     * Returns the connection to the DBMS. The user is responsible for closing it.
     *
     * @return
     *      a connection
     * @see Connection
     */
    public Connection getConnection() throws SQLException {
        return connection;
    }

    /**
     * Creates an index of the appropriate type.
     *
     * @param cols
     *     columns contained in the new index to be used
     * @param descending
     *     DESCENDING/ASCENDING indicator for each column
     * @param type
     *     the index type
     * @throws SQLException
     *     if something wrong occurs during the creation
     */
    public Index createIndex(List<Column> cols, List<Boolean> descending, int type) throws SQLException
    {
        throw new RuntimeException("not implemented yet");
    }
}
