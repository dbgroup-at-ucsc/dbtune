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
package edu.ucsc.dbtune.core.optimizers;

import edu.ucsc.dbtune.core.optimizers.plan.SQLStatementPlan;
import java.sql.SQLException;

/**
 * Represents an optimizer of a DBMS system.
 */
public abstract class Optimizer
{
    /**
     * perform an optimization call for a single SQL statement.
     *
     * @param sql
     *      SQL statement
     * @return
     *      an {@link SQLStatementPlan} object describing the results of an optimization call.
     * @throws SQLException
     *      if an error occurs while retrieving the plan
     */
    public abstract SQLStatementPlan explain(String sql) throws SQLException;
}
