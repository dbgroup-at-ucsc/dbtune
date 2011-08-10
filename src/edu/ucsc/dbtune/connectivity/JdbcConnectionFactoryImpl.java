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
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * @author huascar.sanchez@gmail.com (Huascar A. Sanchez)
 */
class JdbcConnectionFactoryImpl implements JdbcConnectionFactory {
    @Override
    public Connection makeConnection(
            String url, String driverClass,
            String username, String password,
            boolean autoCommit
    ) throws SQLException {
        registeringDriver(driverClass);
        final Connection jdbcConnection = DriverManager.getConnection(
                url,
                username,
                password
        );

        jdbcConnection.setAutoCommit(autoCommit);
        return jdbcConnection;
    }

    private static void registeringDriver(String driverClass) throws SQLException {
        try {
            // registering driver.
            Class.forName(driverClass);
        } catch (ClassNotFoundException e) {
            throw new SQLException(e);
        }
    }
}
