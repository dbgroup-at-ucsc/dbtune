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
import java.sql.Connection;

/**
 * {@inheritDoc}
 */
public class DB2Optimizer extends Optimizer
{
    public DB2Optimizer(Connection connection) throws UnsupportedOperationException {
		throw new UnsupportedOperationException("Not implemented yet");
    }
	public SQLStatementPlan explain(String sql)
	{
		throw new UnsupportedOperationException("Not implemented yet");
	}
}
