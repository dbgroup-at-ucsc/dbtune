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
import edu.ucsc.dbtune.metadata.SQLCategory;
import edu.ucsc.dbtune.metadata.Table;
import edu.ucsc.dbtune.spi.core.Console;
import edu.ucsc.dbtune.util.Checks;
import edu.ucsc.dbtune.util.Strings;
import edu.ucsc.dbtune.util.ToStringBuilder;

import java.sql.SQLException;
import java.sql.Connection;

import static edu.ucsc.dbtune.connectivity.DB2Commands.*;
import static edu.ucsc.dbtune.spi.core.Functions.submit;
import static edu.ucsc.dbtune.spi.core.Functions.submitAll;
import static edu.ucsc.dbtune.spi.core.Functions.supplyValue;
/**
 * Not implemented yet
 */
public class DB2Optimizer extends Optimizer
{
    private final Connection connection;

    private final static Console SCREEN = Console.streaming();

    public DB2Optimizer(Connection connection){
        super();
        this.connection = connection;
    }

    @Override
	public PreparedSQLStatement explain(String sql) throws SQLException
	{
		throw new SQLException("Not implemented yet");
	}

    @Override
    public ExplainInfo explain(String sql, Iterable<? extends Index> indexes) throws SQLException {
        Checks.checkSQLRelatedState(null != connection && !connection.isClosed(), "Connection is closed.");
        Checks.checkArgument(!Strings.isEmpty(sql), "Empty SQL statement");

        SQLCategory category     = null;
        Table       updatedTable = null;
        double      updateCost   = 0.0;

        try {
            // a batch supplying of commands with no returned value.
            submitAll(
                    // clear out the tables we'll be reading
                    submit(clearExplainObject(), connection),
                    submit(clearExplainStatement(), connection),
                    submit(clearExplainOperator(), connection),
                    // execute statment in explain mode = explain
                    submit(explainModeExplain(), connection)
            );

            submit(explainModeNo(), connection);
            category = supplyValue(fetchExplainStatementType(), connection);
            if(SQLCategory.DML.isSame(category)){
                updatedTable = supplyValue(fetchExplainObjectUpdatedTable(), connection);
                updateCost   = supplyValue(fetchExplainOpUpdateCost(), connection);
            }
        } catch (RuntimeException e){
            connection.rollback();
        }

        try {
            connection.rollback();
        } catch (SQLException s){
            error("Could not rollback transaction", s);
            throw s;
        }

        return new DB2ExplainInfo(category, updatedTable, updateCost);
    }

    private static void error(String message, Throwable cause){
        SCREEN.error(message, cause);
    }

    @Override
    public String toString() {
        return new ToStringBuilder<DB2Optimizer>(this)
               .toString();
    }
}
