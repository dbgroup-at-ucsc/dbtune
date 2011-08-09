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

import java.sql.Connection;
import java.sql.SQLException;

/**
 * @author huascar.sanchez@gmail.com (Huascar A. Sanchez)
 */
public interface JdbcConnectionFactory {
    /**
     * makes a jdbc connection given a connection url, username, password,
     * and whether this connection allows auto-commit.
     * @param url connection url.
     * @param driverClass fully qualified name of driver class.
     * @param username user's username.
     * @param password user's password.
     * @param autoCommit
     *      indicates whether the connection will do auto commit or not.
     * @return
     *      a new {@link java.sql.Connection jdbc connection}.
     * @throws java.sql.SQLException
     *      unable to create connection due to an unexpected error.
     */
    Connection makeConnection(String url, String driverClass,  
                              String username, String password,
                              boolean autoCommit
    ) throws SQLException;
}
