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
import java.sql.Statement;

/**
 * @author hsanchez@cs.ucsc.edu (Huascar A. Sanchez)
 */
public class JdbcConnection extends AbstractDatabaseConnection implements DatabaseConnection {
    /**
     * construct a new {@code DefaultDatabaseConnection} object.
     * @param sqlConnection
     *      a {@link java.sql.Connection} instance.
     */
    public JdbcConnection(Connection sqlConnection){
        super(sqlConnection);
        setStatement(makeStatement(sqlConnection));
    }

    private static Statement makeStatement(Connection connection){
        try {
            return connection.createStatement();
        } catch (SQLException e) {
            return null;
        }
    }

    @Override
    public void init() throws Exception {
        getIndexExtractor().adjust(this);
    }
}
