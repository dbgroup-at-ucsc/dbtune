package edu.ucsc.dbtune.inum;

import edu.ucsc.dbtune.core.DatabaseConnection;
import edu.ucsc.dbtune.core.metadata.Configuration;
import edu.ucsc.dbtune.spi.core.Console;

/**
 * Default implementation of {@link OptimalPlanProvider}.
 *
 * @author hsanchez@cs.ucsc.edu (Huascar A. Sanchez)
 */
public class SqlExecutionPlanProvider implements OptimalPlanProvider {
  private final DatabaseConnection connection;

  public SqlExecutionPlanProvider(DatabaseConnection connection) {
    this.connection = connection;
  }

  @Override public String getSqlExecutionPlan(String sql, Configuration inputConfiguration) {
    // todo(Huascar) implement this
    // example of a possible suggested plan
    Console.streaming().info(String.format("%s, %s, %s", connection.getJdbcConnection(), sql, inputConfiguration));
    return "Hash Join  (cost=174080.39..9364262539.50 rows=1 width=193)";   // we can have one or many query plans
  }
}
