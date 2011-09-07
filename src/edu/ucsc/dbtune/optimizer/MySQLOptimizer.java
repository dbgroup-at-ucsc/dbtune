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
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied  *
 *   See the License for the specific language governing permissions and      *
 *   limitations under the License.                                           *
 * ************************************************************************** */
package edu.ucsc.dbtune.optimizer;

import edu.ucsc.dbtune.metadata.Configuration;
import edu.ucsc.dbtune.metadata.Catalog;

import java.sql.Connection;
import java.sql.SQLException;

import edu.ucsc.dbtune.workload.SQLStatement;

/**
 * The interface to the MySQL optimizer.
 *
 * @author Ivo Jimenez
 */
public class MySQLOptimizer extends Optimizer
{
    /**
     * Creates a new optimizer for MySQL.
     *
     * @param connection
     *     JDBC connection used to communicate to a PostgreSQL system.
     * @throws SQLException
     *     if an error occurs while communicating to the server.
     */
    public MySQLOptimizer(Connection connection) throws SQLException
    {
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public PreparedSQLStatement explain(SQLStatement sql, Configuration indexes) throws SQLException {
        throw new SQLException("Not implemented yet");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Configuration recommendIndexes(SQLStatement sql) throws SQLException {
        throw new SQLException("Not implemented yet");
    }
}
