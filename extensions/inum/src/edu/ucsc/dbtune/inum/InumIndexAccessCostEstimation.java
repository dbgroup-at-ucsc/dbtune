package edu.ucsc.dbtune.inum;

import edu.ucsc.dbtune.metadata.Configuration;
import edu.ucsc.dbtune.metadata.Index;

import java.sql.Connection;
import java.util.Set;

/**
 * Default implementation of {@link IndexAccessCostEstimation} interface.
 *
 * @author hsanchez@cs.ucsc.edu (Huascar A. Sanchez)
 */
public class InumIndexAccessCostEstimation implements IndexAccessCostEstimation
{
  private final OptimalPlanProvider provider;
  private final OptimalPlansParser  parser;

  InumIndexAccessCostEstimation(OptimalPlanProvider provider,
      OptimalPlansParser parser){
    this.provider   = provider;
    this.parser     = parser;
  }

  public InumIndexAccessCostEstimation(Connection connection)
  {
    this(new SqlExecutionPlanProvider(connection), new InumOptimalPlansParser());
  }

  @Override public double estimateIndexAccessCost(String query, Configuration indexes)
 {
    // this method will call the optimizer and then get the index access cost
    // per index in the Iterable<DBIndex> object.
    final String      optPlan    = provider.getSqlExecutionPlan(query, indexes);
    final OptimalPlan singlePlan = singlePlan(optPlan);
    double sumOfIndexAccessCosts = 0.0;
    for (Index each : indexes.toList() ){
      sumOfIndexAccessCosts +=  singlePlan.getAccessCost(each.getTable().getName());
    }
    return sumOfIndexAccessCosts;
  }

  private OptimalPlan singlePlan(String returnedPlan)
  {
    // the assumption is that we will get one plan.....
    final Set<OptimalPlan> plans = parser.parse(returnedPlan);
    OptimalPlan plan = null;
    for (OptimalPlan each : plans){
      if (plan == null) { plan = each; }
      else             { break;       }
    }

    return  plan;
  }
}
