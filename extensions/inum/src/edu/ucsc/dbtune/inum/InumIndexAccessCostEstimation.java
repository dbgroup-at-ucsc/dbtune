package edu.ucsc.dbtune.inum;

import edu.ucsc.dbtune.core.DBIndex;
import edu.ucsc.dbtune.core.DatabaseConnection;
import edu.ucsc.dbtune.spi.core.Console;
import java.sql.Connection;
import java.util.Set;

/**
 * Default implementation of {@link IndexAccessCostEstimation} interface.
 *
 * @author hsanchez@cs.ucsc.edu (Huascar A. Sanchez)
 */
public class InumIndexAccessCostEstimation implements IndexAccessCostEstimation {
  private final DatabaseConnection connection;
  private final OptimalPlansParser parser;

  InumIndexAccessCostEstimation(DatabaseConnection connection, OptimalPlansParser parser){
    this.connection = connection;
    this.parser     = parser;
  }

  public InumIndexAccessCostEstimation(DatabaseConnection connection){
    this(connection, new InumOptimalPlansParser());
  }

  @Override public double estimateIndexAccessCost(String query, Iterable<DBIndex> indexes) {
    // this method will call the optimizer and then get the index access cost
    // per index in the Iterable<DBIndex> object.
    final OptimalPlan singlePlan = singlePlan(getSqlExecutionPlan(connection.getJdbcConnection(), query, indexes));
    double sumOfIndexAccessCosts = 0.0;
    for(DBIndex each : indexes ){
      sumOfIndexAccessCosts +=  singlePlan.getAccessCost(each.baseTable().toString()/*todo(Huascar) this should be changed to index.getTablename() */);
    }
    return sumOfIndexAccessCosts;
  }

  private OptimalPlan singlePlan(String returnedPlan){
    // the assumption is that we will get one plan.....
    final Set<OptimalPlan> plans = parser.parse(returnedPlan);
    OptimalPlan plan = null;
    for(OptimalPlan each : plans){
      if(plan == null) { plan = each; }
      else             { break;       }
    }

    return  plan;
  }
  private static String getSqlExecutionPlan(Connection jdbcConnection, String query,
      Iterable<DBIndex> indexes) {
    Console.streaming().info(String.format("%s, %s, %s", jdbcConnection, query, indexes));
    return "Hash Join  (cost=174080.39..9364262539.50 rows=1 width=193)";
  }

}
