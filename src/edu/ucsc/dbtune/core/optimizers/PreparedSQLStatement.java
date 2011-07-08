/* **************************************************************************** *
 *   Copyright 2010 University of California Santa Cruz                         *
 *                                                                              *
 *   Licensed under the Apache License, Version 2.0 (the "License");            *
 *   you may not use this file except in compliance with the License.           *
 *   You may obtain a copy of the License at                                    *
 *                                                                              *
 *       http://www.apache.org/licenses/LICENSE-2.0                             *
 *                                                                              *
 *   Unless required by applicable law or agreed to in writing, software        *
 *   distributed under the License is distributed on an "AS IS" BASIS,          *
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.   *
 *   See the License for the specific language governing permissions and        *
 *   limitations under the License.                                             *
 * **************************************************************************** */
package edu.ucsc.dbtune.core.optimizers;

import edu.ucsc.dbtune.core.metadata.SQLCategory;
import edu.ucsc.dbtune.core.metadata.Configuration;
import edu.ucsc.dbtune.core.optimizers.plan.SQLStatementPlan;
import edu.ucsc.dbtune.util.ToStringBuilder;

/**
 * Represents a SQL statement that has been optimized. Each {@code SQLStatement} object is tied to a 
 * {@link Configuration} corresponding to the physical design considered by the optimizer to 
 * estimate the cost of the statement.
 *
 * @see Optimizer
 */
public class SQLStatement
{
    /** type of statement */
    protected SQLCategory type;

    /** literal contents of the statement */
    protected String sql;

    /** cost assigned by an {@link Optimizer} */
    protected double cost;

    /** configuration that was used to optimize the statement */
    // Note: if tying a configuration breaks orthogonality (by introducing a dependency with 
    // dbtune.core.metadata.Configuration), this class can be refactored by creating an abstract 
    // class (which will be named SQLStatement) that is orthogonal and then have this one (renamed 
    // to OptimizedSQLStatement) as its child.
    protected Configuration configuration;

    /** the optimized plan */
    protected SQLStatementPlan plan;

    /**
     * Constructs a {@code SQLStatement} given its type and the literal contents.
     *
     * @param category
     *      the corresponding {@link SQLCategory} representing the type of statement.
     * @param sql
     *      a sql statement.
     */
    public SQLStatement(SQLCategory category, String sql) {
        this.type = category;
        this.sql  = sql;
    }

    /**
     * Returns the type of statement.
     *
     * @return
     *     a sql category.
     * @see SQLCategory
     */
    public SQLCategory getSQLCategory() {
        return type;
    }

    /**
     * Returns the actual SQL statement.
     *
     * @return
     *     a string containing the SQL statement that was optimized.
     */
    public String getSQL() {
        return sql;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        return new ToStringBuilder<SQLStatement>(this)
               .add("type", getSQLCategory())
               .add("sql", getSQL())
               .toString();
    }
}
