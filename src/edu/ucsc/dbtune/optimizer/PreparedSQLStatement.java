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
package edu.ucsc.dbtune.optimizer;

import edu.ucsc.dbtune.metadata.Configuration;
import edu.ucsc.dbtune.optimizer.plan.SQLStatementPlan;
import edu.ucsc.dbtune.workload.SQLStatement;

/**
 * Represents a SQL statement that has been optimized. Each {@code SQLStatement} object is tied to a 
 * {@link Configuration} corresponding to the physical design considered by the optimizer at the 
 * time that the execution cost was estimated.
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
     * @param statement
     *      the corresponding {@link edu.ucsc.dbtune.workload.SQLStatement} representing the actual SQL statement.
     * @param cost
     *      cost sql statement.
     * @param configuration
     *      configuration used to optimize the statement.
     */
    public PreparedSQLStatement(SQLStatementPlan plan, double cost, Configuration configuration) {
        this.statement     = plan.getStatement();
		this.plan          = plan;
        this.cost          = cost;
        this.configuration = configuration;
    }

	/**
	 * Returns the cost of executing the statement.
	 *
	 * @return
	 *      the execution cost of the statement.
     */
	public double getCost()
	{
		return cost;
	}
}
