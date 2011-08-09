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

import java.sql.SQLException;

import edu.ucsc.dbtune.core.DatabaseSystem;

/**
 * manages a set of {@link DatabaseConnection} instances.  A database connection is established by
 * invoking {@link ConnectionManager#connect()}. All the connections managed by this connection manager can be
 * closed using {@link #close()}.
 *
 * @author hsanchez@cs.ucsc.edu (Huascar A. Sanchez)
 */
public interface ConnectionManager {
    /**
     * @return the name of the used database.
     */
    String getDatabaseName();

    /**
     * @return either {@link DatabaseSystem#POSTGRES} or
     *      {@link DatabaseSystem#DB2}, which are the currently
     *      supported {@link DatabaseSystem}s.
     */
    DatabaseSystem getDatabaseSystem();

    /**
	 * sets the connection manager's state to <em>open</em> by establishing a new database connection
     * (assumming this is the first connection). The state won't be updated if more than one connection is
     * created by this manager.
	 * @return
     *      an object that will complete when the database connection has been established or failed.
     * @throws java.sql.SQLException
     *      thrown if we were unable to connect to a database system.
     */
    DatabaseConnection connect() throws SQLException;

    /**
	 * closes all the database connections managed by this {@code DatabaseConnectionManager} and
     * releases any resources used for managing these database connections.
	 * @throws java.sql.SQLException
     *        if there's an error closing all the database connections
     */
    void close() throws SQLException;

    /**
     * closes a single connection.
     * If the connection to be closed is the only connection, then the {@code connection manager}
     * will be closed too.
     * @param connection
     *      single connection to be closed.
     */
    void close(DatabaseConnection connection);

	/**
	 * Indicates if this {@code DatabaseConnectionManager} is closed.
	 * @return  {@code true}
     *      if this {@code DatabaseConnectionManager} is closed, false otherwise.
	 */
	boolean isClosed();

	/**
	 * Indicates if this {@code DatabaseConnectionManager} is opened.
	 * @return  {@code true}
     *      if this {@code DatabaseConnectionManager} is opened, {@code false} otherwise.
	 */
    boolean isOpened();
}