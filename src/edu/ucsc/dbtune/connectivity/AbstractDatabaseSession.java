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

package edu.ucsc.dbtune.connectivity;

import edu.ucsc.dbtune.util.Checks;
import edu.ucsc.dbtune.util.ToStringBuilder;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.atomic.AtomicReference;

/**
 * This class provides a skeletal implementation of the {@link DatabaseSession}
 * interface to minimize the effort required to implement this interface.
 *
 * @author huascar.sanchez@gmail.com (Huascar A. Sanchez)
 */
abstract class AbstractDatabaseSession implements DatabaseSession {
    protected final AtomicReference<Statement>  sqlStatement;
    protected final AtomicReference<Connection> sqlConnection;


    /**
     * construct an {@code AbstractDatabaseSession} instance. This constructor is
     * intended to be called by this class's subclasses.
     * @param connection
     *      a java.sql.Connection.
     */
    protected AbstractDatabaseSession(Connection connection){
        sqlStatement  = new AtomicReference<Statement>();
        sqlConnection = new AtomicReference<Connection>(connection);
    }

    /**
     * a method intended to clean up any used resources after closing a db connection.
     * @throws IllegalStateException
     *      if there is a violation of any of the invariants assummend by this
     *      class's subclasses.
     * @throws java.sql.SQLException
     *      an error has occured while talking to db.
     */
    void cleanup() throws SQLException {
        sqlConnection.set(null);
        sqlStatement.set(null);
    }

    @Override
    public void commit() throws SQLException {
        ensureSessionIsOpened();
        sqlConnection.get().commit();
    }

    @Override
    public void close() throws SQLException {
        if(isClosed()) return;
        try {
            sqlConnection.get().close();
        } finally {
            cleanup();
        }
    }

    /**
     * ensure that we have an opened session.
     * @throws java.sql.SQLException
     *      throw an error if we are trying to use a closed session.
     */
    protected void ensureSessionIsOpened() throws SQLException {
        Checks.checkSQLRelatedState(isOpened(), "cannot execute: no database connection");
    }

    @Override
    public void execute(String sql) throws SQLException {
        Checks.checkArgument(sql != null);
        Checks.checkSQLRelatedState(sql != null && !sql.isEmpty());
        ensureSessionIsOpened();
        try {
           init();  //todo(Huascar) if we are setting the isolation read committed in JDBC connection
           executeQuery(sql);
        } catch(Exception cause){
            throw new SQLException(cause);
        }
    }

    /**
     * executes a sql query given the assumption that we have an active connection to
     * an existing database. To guarantee this assumption, the user must have created a
     * new instance of this statement when calling {@link #init()}.
     * @param sql
     *      sql query to be executed.
     * @return
     *      {@code true} if the query was successfully executed, false otherwise.
     * @throws java.sql.SQLException
     *      an error occurred; the query was not executed.
     */
    // package private to help testing.
    boolean executeQuery(String sql) throws SQLException {
        final Statement statement = Checks.checkNotNull(sqlStatement.get(), "error: failed to initialize session.");
        return statement.execute(sql);
    }

    @Override
    public Connection getJdbcConnection(){
        return Checks.checkNotNull(sqlConnection.get());
    }


    /**
     * a hook method where more application specific pre-conditions could be placed or any resources
     * needed to establish a session could be instantiated.
     *
     * @throws Exception
     *      unable to fulfill the contract of this method.
     */
	public abstract void init() throws Exception;

    @Override
    public boolean isClosed() {
        if (sqlConnection.get() == null){
          return true;
        } else {
            try {
                return sqlConnection.get().isClosed();
            } catch (SQLException e) {
                return false;
            }
        }

    }

    @Override
    public boolean isOpened() {
        return !(isClosed());
    }

    @Override
    public void rollback() throws SQLException {
        ensureSessionIsOpened();
        sqlConnection.get().rollback();
    }

    /**
     * sets the statement needed to execute queries.
     * @param value
     *      new {@link java.sql.Statement} object.
     */
    protected final void setStatement(Statement value){
        sqlStatement.compareAndSet(null, value);
    }


    @Override
    public String toString() {
        return new ToStringBuilder<AbstractDatabaseSession>(this)
                .add("open", isOpened())
                .toString();
    }
}