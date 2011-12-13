package edu.ucsc.dbtune.inum;

import java.util.List;

/**
 * A cached plan wraps a plan suggested by the optimizer and flips
 * the {@link OptimalPlan#isDirty() dirty flag} from dirty to not dirty (meaning
 * the plan has been cached). This makes individual {@link OptimalPlan} immutable. This
 * is a better approach than the one where you have to add some setters to the OptimalPlan
 * interface. Plus, dealing with immutable objects makes testing easier.
 *
 * @author hsanchez@cs.ucsc.edu (Huascar A. Sanchez)
 */
public class CachedSqlExecutionOptimalPlan implements OptimalPlan
{
  private final OptimalPlan plan;

  public CachedSqlExecutionOptimalPlan(OptimalPlan plan)
  {
    this.plan = plan;
  }

  @Override public boolean addSubplan(Subplan subplan)
 {
    return plan.addSubplan(subplan);
  }

  @Override public void computeInternalPlanCost()
 {
    plan.computeInternalPlanCost();
  }

  @Override public List<Subplan> getInternalPlans()
 {
    return plan.getInternalPlans();
  }

  @Override public double getTotalCost()
 {
    return plan.getTotalCost();
  }

  @Override public double getAccessCost(String tableName)
 {
    return plan.getAccessCost(tableName);
  }

  @Override public double getInternalCost()
 {
    return plan.getInternalCost();
  }

  @Override public boolean isDirty()
 {
    return !plan.isDirty();
  }

  @Override public boolean removeSubplan(Subplan subplan)
 {
    return plan.removeSubplan(subplan);
  }

  @Override public void setAccessCost(String tableName, double cost)
 {
    plan.setAccessCost(tableName, cost);
  }

  @Override public String toString()
 {
    return plan.toString();
  }
}
