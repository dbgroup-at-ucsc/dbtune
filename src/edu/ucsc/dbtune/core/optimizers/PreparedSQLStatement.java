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

import edu.ucsc.dbtune.workload.SQLStatement;
import edu.ucsc.dbtune.core.metadata.Configuration;
import edu.ucsc.dbtune.core.optimizers.plan.SQLStatementPlan;

/**
 * Represents a SQL statement that has been optimized. Each {@code SQLStatement} object is tied to a 
 * {@link Configuration} corresponding to the physical design considered by the optimizer to 
 * estimate the cost of the statement.
 *
 * @see    Optimizer
 * @author Ivo Jimenez
 */
public class PreparedSQLStatement
{
    /** statement corresponding to this prepared statement, i.e. SQL statement that was prepared */
    SQLStatement statement;

    /** configuration that was used to optimize the statement */
    protected Configuration configuration;

    /** cost assigned by an {@link Optimizer} */
    protected double cost;

    /** the optimized plan */
    protected SQLStatementPlan plan;

    /**
     * Constructs a {@code PreparedSQLStatement} given its corresponding statement and the cost 
     * assigned to it.
     *
     * @param stmt
     *      the corresponding {@link SQLStatment} representing the actual SQL statement.
     * @param cost
     *      cost sql statement.
     * @param configuration
     *      configuration used to optimize the statement.
     */
    public PreparedSQLStatement(SQLStatement statement, double cost, Configuration configuration) {
        this.statement     = statement;
        this.cost          = cost;
        this.configuration = configuration;
    }
}
