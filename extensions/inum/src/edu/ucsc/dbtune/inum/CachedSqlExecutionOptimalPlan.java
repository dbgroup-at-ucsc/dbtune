package edu.ucsc.dbtune.inum;

import java.util.List;
import java.util.Set;

import com.google.common.base.Preconditions;

import edu.ucsc.dbtune.metadata.Index;

/**
 * A cached plan wraps a plan suggested by the optimizer and flips the {@link OptimalPlan#isDirty()
 * dirty flag} from dirty to not dirty (meaning the plan has been cached). This makes individual
 * {@link OptimalPlan} immutable. This is a better approach than the one where you have to add some
 * setters to the OptimalPlan interface. Plus, dealing with immutable objects makes testing easier.
 *
 * @author hsanchez@cs.ucsc.edu (Huascar A. Sanchez)
 */
public class CachedSqlExecutionOptimalPlan implements OptimalPlan {
  private static final String ERROR_MESSAGE
      = "Invalid argument. Expected a subclass of SQLExecutionOptimalPlan. Found one of CachedSQLExecutionOptimalPlan";
  private final OptimalPlan plan;

  public CachedSqlExecutionOptimalPlan(OptimalPlan plan) {
    Preconditions.checkArgument(!(plan instanceof CachedSqlExecutionOptimalPlan), ERROR_MESSAGE);
    this.plan = plan;
  }

  @Override public boolean add(PhysicalOperator operator) {
    return plan.add(operator);
  }

  @Override public void fixAccessCosts(Set<Index> configuration, QueryRecord queryRecord) {
    plan.fixAccessCosts(configuration, queryRecord);
  }

  @Override public void computeInternalPlanCost() {
    plan.computeInternalPlanCost();
  }

  @Override public List<PhysicalOperator> getInternalPlans() {
    return plan.getInternalPlans();
  }

  @Override public double getTotalCost() {
    return plan.getTotalCost();
  }

  @Override public double getAccessCost(String tableName) {
    return plan.getAccessCost(tableName);
  }

  @Override public double getInternalCost() {
    return plan.getInternalCost();
  }

  @Override public boolean isDirty() {
    return !plan.isDirty();
  }

  @Override public boolean remove(PhysicalOperator operator) {
    return plan.remove(operator);
  }

  @Override public void setAccessCost(String tableName, double cost) {
    plan.setAccessCost(tableName, cost);
  }

  @Override public String toString() {
    return plan.toString();
  }
}
