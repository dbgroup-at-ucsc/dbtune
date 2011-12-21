package edu.ucsc.dbtune.inum;

import java.sql.Connection;
import java.util.Set;

import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.spi.Console;

/**
 * Default implementation of {@link OptimalPlanProvider}.
 *
 * @author Huascar A. Sanchez
 */
public class SqlExecutionPlanProvider implements OptimalPlanProvider
{
    private final Connection connection;

    public SqlExecutionPlanProvider(Connection connection)
    {
        this.connection = connection;
    }

    @Override
    public String getSqlExecutionPlan(String sql, Set<Index> inputConfiguration)
    {
        // todo(Huascar) implement this
        // example of a possible suggested plan
        Console.streaming().info(String.format("%s, %s, %s", connection, sql, inputConfiguration));
        return "Hash Join  (cost=174080.39..9364262539.50 rows=1 width=193)";   // we can have one or many query plans
    }
}
