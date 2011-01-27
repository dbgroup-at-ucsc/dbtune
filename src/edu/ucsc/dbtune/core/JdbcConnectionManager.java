/*
 * ****************************************************************************
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
 *  ****************************************************************************
 */

package edu.ucsc.dbtune.core;

import edu.ucsc.dbtune.util.Files;
import edu.ucsc.dbtune.util.Checks;
import edu.ucsc.satuning.util.ToStringBuilder;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

/**
 * @author huascar.sanchez@gmail.com (Huascar A. Sanchez)
 */
public final class JdbcConnectionManager extends AbstractConnectionManager
        implements ConnectionManager {

    public static final String USERNAME         = "username";
    public static final String PASSWORD         = "password";
    public static final String URL              = "url";
    public static final String DRIVER           = "driver";
    public static final String PASSWORD_FILE    = "passwordFile";
    public static final String DATABASE         = "database";
    public static final String ADVISOR_FOLDER   = "advisorFolder";


    private final String                            jdbcUrl;
    private final Set<DatabaseConnection>           connections             = new HashSet<DatabaseConnection>();
    private final String                            driverClass;
    private final JdbcConnectionFactory             jdbcConnectionFactory;
    private final DatabaseSystem                    databaseSystem;

    private JdbcConnectionManager(
            String url,
            String username,
            String password,
            String database,
            String driverClass,
            DatabaseSystem databaseSystem,
            JdbcConnectionFactory jdbcConnectionFactory
    ) {
        super(username, password, database);
        this.jdbcConnectionFactory  = Checks.checkNotNull(jdbcConnectionFactory);
        this.driverClass            = Checks.checkNotNull(driverClass);
        this.jdbcUrl                = Checks.checkNotNull(url);
        this.databaseSystem         = Checks.checkNotNull(databaseSystem);

    }

    @Override
    public DatabaseConnection connect() throws SQLException {
        final Connection  jdbcConnection  = jdbcConnectionFactory.makeConnection(
                jdbcUrl,
                driverClass,
                getUsername(),
                getPassword(),
                false
        );
        
        final JdbcConnection nConn  = new JdbcConnection(jdbcConnection);

        // the database index extractor will be closed if it does not have any opened connections.
        connections.add(nConn);
        nConn.createdBy(this);
        nConn.loadResources();
        return nConn;
    }

    @Override
    public void close(DatabaseConnection connection) {
        if(!connections.contains(connection)) return;
        connections.remove(connection);
        try {
            if(connection.isOpened()){
                connection.close();
            }
        } catch (SQLException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    public void close() throws SQLException {
        for(DatabaseConnection each :
                // this is needed, otherwise we will get a ConcurrentAccess Exception
                new HashSet<DatabaseConnection>(connections)){
            if(isOpened()){
                each.close();
            }
        }
        connections.clear();
    }

    @Override
    public boolean isClosed() {
        return (connections.isEmpty());
    }

    @Override
    public boolean isOpened() {
        return !isClosed();
    }

    private static String getPassword(Properties props){
        if(props.containsKey(PASSWORD_FILE)){
            final File file = new File(props.getProperty(PASSWORD_FILE));
            try {
                return Files.readFile(file);
            } catch (IOException e) {
                throw new IllegalArgumentException(e);
            }
        } else {
            return props.getProperty(PASSWORD);
        }
    }


    @Override
    public DatabaseSystem getDatabaseSystem() {
        return databaseSystem;
    }

    /**
     * construct a new {@link ConnectionManager} given a property object.
     * the following snippet shows you the use of the JdbcConnectionManager for creating
     * {@link ConnectionManager}s.
     *
     * <pre>
     * final Properties p = new Properties();
     * p.setProperty(JdbcDatabaseConnectionManager.URL, "some url");
     * p.setProperty(JdbcDatabaseConnectionManager.USERNAME, "neo");
     * p.setProperty(JdbcDatabaseConnectionManager.PASSWORD, "password");
     * p.setProperty(JdbcDatabaseConnectionManager.PASSWORD_FILE, "password file"); // will override the previous property.
     * p.setProperty(JdbcDatabaseConnectionManager.DRIVER, "jdbc driver");
     * p.setProperty(JdbcDatabaseConnectionManager.DATABASE, "some url");
     *
     * final DatabaseConnectionManager<I> myCM = JdbcDatabaseConnectionManager.makeConnectionManager(p);
     *
     * final DatabaseConnection<I> connection = myCM.connect();
     * .....
     * </pre>
     * @param props
     *      connection's properties.
     * @return
     *      a new {@link ConnectionManager} instance.
     * @throws IllegalArgumentException
     *      if not valid file path has been included for a given Password File property.
     */
    public static ConnectionManager makeDatabaseConnectionManager(Properties props){
        return makeDatabaseConnectionManager(props, new JdbcConnectionFactoryImpl());
    }

    /**
     * construct a new {@link ConnectionManager} given a property object.
     * the following snippet shows you the use of the JdbcConnectionManager for creating
     * {@link ConnectionManager}s.
     *
     * <pre>
     * final Properties p = new Properties();
     * p.setProperty(JdbcDatabaseConnectionManager.URL, "some url");
     * p.setProperty(JdbcDatabaseConnectionManager.USERNAME, "neo");
     * p.setProperty(JdbcDatabaseConnectionManager.PASSWORD, "password");
     * p.setProperty(JdbcDatabaseConnectionManager.PASSWORD_FILE, "password file"); // will override the previous property.
     * p.setProperty(JdbcDatabaseConnectionManager.DRIVER, "jdbc driver");
     * p.setProperty(JdbcDatabaseConnectionManager.DATABASE, "some url");
     *
     * final DatabaseConnectionManager<I> myCM = JdbcDatabaseConnectionManager.makeConnectionManager(p);
     *
     * final DatabaseConnection<I> connection = myCM.connect();
     * .....
     * </pre>
     * @param props
     *      connection's properties.
     * @param jdbcConnectionFactory
     *      factory object that creates JDBC connections (e.g., java.sql.Connection).
     * @return a new {@link ConnectionManager} instance.
     */
    public static ConnectionManager makeDatabaseConnectionManager(Properties props, JdbcConnectionFactory jdbcConnectionFactory){
        final String password    = getPassword(props);
        final String driverClass = props.getProperty(DRIVER);
        return new JdbcConnectionManager(
                props.getProperty(URL),
                props.getProperty(USERNAME),
                password,
                props.getProperty(DATABASE),
                driverClass,
                DatabaseSystem.fromQualifiedName(driverClass),
                jdbcConnectionFactory
        );
    }

    @Override
    public String toString() {
        return new ToStringBuilder<JdbcConnectionManager>(this)
               .add("DBMS", getDatabaseSystem())
               .add("Connections Count", connections.size())
               .add("Active", isOpened())
               .toString();
    }

}
