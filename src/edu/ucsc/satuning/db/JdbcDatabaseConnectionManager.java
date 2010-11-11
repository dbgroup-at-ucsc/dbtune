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
package edu.ucsc.satuning.db;

import edu.ucsc.satuning.util.Files;
import edu.ucsc.satuning.util.PreConditions;

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
public final class JdbcDatabaseConnectionManager<I extends DBIndex<I>> extends AbstractDatabaseConnectionManager<I>
implements DatabaseConnectionManager<I> {

    public static final String USERNAME         = "username";
    public static final String PASSWORD         = "password";
    public static final String URL              = "url";
    public static final String DRIVER           = "driver";
    public static final String PASSWORD_FILE    = "passwordFile";
    public static final String DATABASE         = "database";


    private final String                            jdbcUrl;
    private final Set<DatabaseConnection<I>>        connections     = new HashSet<DatabaseConnection<I>>();
    private final DatabaseIndexExtractorFactory<I>  indexExtractorFactory;
    private final DatabaseWhatIfOptimizerFactory<I> whatIfOptimizerFactory;
    private final String                            driverClass;
    private final JdbcConnectionFactory             jdbcConnectionFactory;
    private final AtomicBoolean                     closed;

    private JdbcDatabaseConnectionManager(
            String url,
            String username,
            String password,
            String database,
            String driverClass,
            DatabaseIndexExtractorFactory<I>  indexExtractorFactory,
            DatabaseWhatIfOptimizerFactory<I> databaseWhatIfOptimizer,
            JdbcConnectionFactory jdbcConnectionFactory
    ) {
        super(username, password, database);
        this.jdbcConnectionFactory  = PreConditions.checkNotNull(jdbcConnectionFactory);
        this.driverClass            = PreConditions.checkNotNull(driverClass);
        this.jdbcUrl                = PreConditions.checkNotNull(url);
        this.indexExtractorFactory  = PreConditions.checkNotNull(indexExtractorFactory);
        this.whatIfOptimizerFactory = PreConditions.checkNotNull(databaseWhatIfOptimizer);

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
     * @param <I>
     *      the type of {@code DBIndex}.
     * @return
     *      a new {@link DatabaseConnectionManager} instance.
     * @throws IllegalArgumentException
     *      if not valid file path has been included for a given Password File property.
     */
    public static <I extends DBIndex<I>> DatabaseConnectionManager<I> makeDatabaseConnectionManager(Properties props){
        return makeDatabaseConnectionManager(props, new JdbcConnectionFactoryImpl());
    }

    // this was added for testing purposes.
    static <I extends DBIndex<I>> DatabaseConnectionManager<I> makeDatabaseConnectionManager(Properties props, JdbcConnectionFactory jdbcConnectionFactory){
        final String password    = getPassword(props);
        final String driverClass = props.getProperty(DRIVER);
        return new JdbcDatabaseConnectionManager<I>(
                props.getProperty(URL),
                props.getProperty(USERNAME),
                password,
                props.getProperty(DATABASE),
                driverClass,
                Platform.<I>findIndexExtractorFactory(driverClass),
                Platform.<I>findWhatIfOptimizerFactory(driverClass),
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
    public DatabaseConnection<I> connect() throws SQLException {
        final Connection  jdbcConnection  = jdbcConnectionFactory.makeConnection(
                jdbcUrl,
                driverClass,
                getUsername(),
                getPassword(),
                false
        );
        final DefaultDatabaseConnection<I>  nConn  = new DefaultDatabaseConnection<I>(
                this,
                jdbcConnection
        );

        final DatabaseIndexExtractor<I>  indexExtractor  = indexExtractorFactory.makeIndexExtractor(nConn);
        final DatabaseWhatIfOptimizer<I> whatIfOptimezer = whatIfOptimizerFactory.makeWhatIfOptimizer(nConn);

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
    public void close(DatabaseConnection<I> connection) {
        if(!connections.contains(connection)) return;
        connections.remove(connection);
        try {
            if(connection.isOpened()){
                connection.close();
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() throws SQLException {
        for(DatabaseConnection<?> each :
                // this is needed, otherwise we will get a ConcurrentAccess Exception
                new HashSet<DatabaseConnection<?>>(connections)){
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
