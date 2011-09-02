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

import edu.ucsc.dbtune.metadata.Catalog;
import edu.ucsc.dbtune.metadata.Configuration;
import edu.ucsc.dbtune.workload.SQLStatement;

import java.sql.SQLException;

/**
 * Represents an optimizer of a DBMS system.
 */
public abstract class Optimizer
{
    protected Catalog catalog;

    /**
     * perform an optimization call for a single SQL statement.
     *
     * @param sql
     *      SQL statement
     * @return
     *      an {@link PreparedSQLStatement} object describing the results of an optimization call.
     * @throws SQLException
     *      if an error occurs while retrieving the plan
     */
    public PreparedSQLStatement explain(SQLStatement sql) throws SQLException {
        return explain(sql, new Configuration("empty"));
    }

    /**
     * estimate what-if optimization plan of a statement using the given configuration.
     *
     * @param sql
     *     sql statement
     * @param configuration
     *     physical configuration the optimizer should consider when preparing the statement
     * @return
     *     an {@link PreparedSQLStatement} object describing the results of a what-if optimization 
     *     call.
     * @throws java.sql.SQLException
     *     unable to estimate cost for the stated reasons.
     */
    public abstract PreparedSQLStatement explain(SQLStatement sql, Configuration configuration)
        throws SQLException;
    
    /**
     * Given a sql statement, it recommends indexes to make it run faster.
     *
     * @param sql
     *      SQL statement
     * @return
     *      a list of indexes that would improve the statement's performance
     * @throws SQLException
     *      if an error occurs while retrieving the plan
     */
    public abstract Configuration recommendIndexes(SQLStatement sql) throws SQLException;

    /**
     * Assigns the catalog that should be used to bind metadata to prepared statements.
     *
     * @param catalog
     *     metadata to be used when binding database objects.
     */
    public void setCatalog(Catalog catalog)
    {
        this.catalog = catalog;
    }
}
