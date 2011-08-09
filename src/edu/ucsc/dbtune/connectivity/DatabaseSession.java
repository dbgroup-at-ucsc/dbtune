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

import java.sql.Connection;
import java.sql.SQLException;

/**
 * An immutable type that represents instances of database sessions.
 * @author huascar.sanchez@gmail.com (Huascar A. Sanchez)
 */
public interface DatabaseSession {    
    /**
     * commits a statement.
     * @throws java.sql.SQLException
     *      an unexpected error has occurred.
     */
    void commit() throws SQLException;

    /**
     * close an active db session.
     * @throws java.sql.SQLException
     *     an unexpected error has occurred.
     */
    void close() throws SQLException;

    /**
     * gets a provider of {@literal T}s, which is a type given a compiled time.
     * @param sql
     *      a sql query to be executed.
     * @throws java.sql.SQLException
     *      an unexpected error has occurred.
     */
    void execute(String sql) throws SQLException;
    /**
     * @return a recently given
     *      {@link java.sql.Connection} object.
     */    
    Connection getJdbcConnection();

    /**
     * @return {@code true} if the db session is closed. false otherwise.
     */
    boolean isClosed();

    /**
     * @return {@code true} if the db session is opened. false otherwise.
     */
    boolean isOpened();

    /**
     * rolls back the execution of a statement.
     * @throws java.sql.SQLException
     *      an unexpected error has occurred.
     */
    void rollback() throws SQLException;
}
