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

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author huascar.sanchez@gmail.com (Huascar A. Sanchez)
 */
public final class JdbcDatabaseConnectionManager extends AbstractDatabaseConnectionManager
        implements DatabaseConnectionManager {

    public static final String USERNAME         = "username";
    public static final String PASSWORD         = "password";
    public static final String URL              = "url";
    public static final String DRIVER           = "driver";
    public static final String PASSWORD_FILE    = "passwordFile";
    public static final String DATABASE         = "database";


    private final String                            jdbcUrl;
    private final Set<DatabaseConnection>           connections             = new HashSet<DatabaseConnection>();
    private final DatabaseIndexExtractorFactory     indexExtractorFactory;
    private final DatabaseWhatIfOptimizerFactory    whatIfOptimizerFactory;
    private final String                            driverClass;
    private final JdbcConnectionFactory             jdbcConnectionFactory;
    private final AtomicBoolean                     closed;

    private JdbcDatabaseConnectionManager(
            String url,
            String username,
            String password,
            String database,
            String driverClass,
            DatabaseIndexExtractorFactory indexExtractorFactory,
            DatabaseWhatIfOptimizerFactory databaseWhatIfOptimizer,
            JdbcConnectionFactory jdbcConnectionFactory
    ) {
        super(username, password, database);
        this.jdbcConnectionFactory  = Checks.checkNotNull(jdbcConnectionFactory);
        this.driverClass            = Checks.checkNotNull(driverClass);
        this.jdbcUrl                = Checks.checkNotNull(url);
        this.indexExtractorFactory  = Checks.checkNotNull(indexExtractorFactory);
        this.whatIfOptimizerFactory = Checks.checkNotNull(databaseWhatIfOptimizer);

        closed = new AtomicBoolean(false);
    }

    /**
     * construct a new {@link DatabaseConnectionManager} given a property object.
     * the following snippet shows you the use of the JdbcConnectionManager for creating
     * {@link DatabaseConnectionManager}s.
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
     *      a new {@link edu.ucsc.dbtune.core.DatabaseConnectionManager} instance.
     * @throws IllegalArgumentException
     *      if not valid file path has been included for a given Password File property.
     */
    public static DatabaseConnectionManager makeDatabaseConnectionManager(Properties props){
        return makeDatabaseConnectionManager(props, new JdbcConnectionFactoryImpl());
    }

    public static DatabaseConnectionManager makeDatabaseConnectionManager(Properties props, JdbcConnectionFactory jdbcConnectionFactory){
        final String password    = getPassword(props);
        final String driverClass = props.getProperty(DRIVER);
        return new JdbcDatabaseConnectionManager(
                props.getProperty(URL),
                props.getProperty(USERNAME),
                password,
                props.getProperty(DATABASE),
                driverClass,
                Platform.findIndexExtractorFactory(driverClass),
                Platform.findWhatIfOptimizerFactory(driverClass),
                jdbcConnectionFactory
        );
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
    public DatabaseConnection connect() throws SQLException {
        final Connection  jdbcConnection  = jdbcConnectionFactory.makeConnection(
                jdbcUrl,
                driverClass,
                getUsername(),
                getPassword(),
                false
        );
        final JdbcDatabaseConnection nConn  = new JdbcDatabaseConnection(
                this,
                jdbcConnection
        );

        //todo(Huascar) please provide the advisor folder path
        final DatabaseIndexExtractor indexExtractor   = indexExtractorFactory.makeIndexExtractor("", nConn);
        final DatabaseWhatIfOptimizer whatIfOptimezer = whatIfOptimizerFactory.makeWhatIfOptimizer(nConn);

        nConn.install(
                indexExtractor,
                whatIfOptimezer
        );
        // the database index extractor will be closed if it does not have any opened connections.
        if(isClosed()) {
            open();
        }

        connections.add(nConn);
        return nConn;
    }

    private void open(){
        closed.set(true);
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
        closed.set(true);
    }

    @Override
    public boolean isClosed() {
        return closed.get() && connections.isEmpty();
    }

    @Override
    public boolean isOpened() {
        return !connections.isEmpty();
    }

    @Override
    public String toString() {
        return super.toString();
    }

}
