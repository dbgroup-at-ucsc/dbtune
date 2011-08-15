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

import edu.ucsc.dbtune.metadata.Index;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * Represents an optimizer of a DBMS system.
 */
public abstract class Optimizer
{
    protected int whatIfCount;

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
    public PreparedSQLStatement explain(String sql) throws SQLException {
        return explain(sql, new ArrayList<Index>());
    }

    /**
     * estimate what-if optimization plan of a statement using the given configuration.
     *
     * @param sql
     *     sql statement
     * @param indexes
     *     configuration
     * @return
     *     an {@link ExplainInfo} object describing the results of a
     *     what-if optimization call.
     * @throws java.sql.SQLException
     *     unable to estimate cost for the stated reasons.
     */
    public abstract PreparedSQLStatement explain(String sql, Iterable<? extends Index> indexes) throws SQLException;
    
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
    public abstract List<Index> recommendIndexes(String sql) throws SQLException;

    /**
     * gets the total count of what-if optimizations were handled/performed by the optimizer.
     *
     * @return
     *     the total count of performed what-if optimizations.
     */
    public int getWhatIfCount()
    {
        return whatIfCount;
    }
}
