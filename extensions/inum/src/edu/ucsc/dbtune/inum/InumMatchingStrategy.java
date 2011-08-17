package edu.ucsc.dbtune.inum;

import edu.ucsc.dbtune.core.DBIndex;
import edu.ucsc.dbtune.core.DatabaseConnection;
import java.util.Set;

/**
 * Default implementation of Inum's {@link MatchingStrategy}
 *
 * @author hsanchez@cs.ucsc.edu (Huascar A. Sanchez)
 */
public class InumMatchingStrategy implements MatchingStrategy {
  private final IndexAccessCostEstimation accessCostEstimator;
  private final DatabaseConnection        connection;

  InumMatchingStrategy(IndexAccessCostEstimation accessCostEstimator, DatabaseConnection connection){
    this.accessCostEstimator = accessCostEstimator;
    this.connection = connection;
  }

  public InumMatchingStrategy(DatabaseConnection connection){
    this(new InumIndexAccessCostEstimation(), connection);
  }

  @Override
  public double derivesCost(OptimalPlan optimalPlan, Iterable<DBIndex> inputConfiguration) {
    return 0;  //todo(Huascar) implement this
  }

  @Override
  public OptimalPlan matches(Set<OptimalPlan> optimalPlans, Iterable<DBIndex> inputConfiguration) {
    return null;  //todo(Huascar) implement this
  }
}
