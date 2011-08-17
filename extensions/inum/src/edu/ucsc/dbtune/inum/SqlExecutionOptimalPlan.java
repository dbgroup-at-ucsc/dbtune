package edu.ucsc.dbtune.inum;

import com.google.common.base.Objects;
import com.google.common.collect.Lists;
import java.util.List;

/**
 * The not yet cached Sql Execution Plan.
 *
 * @author hsanchez@cs.ucsc.edu (Huascar A. Sanchez)
 */
public class SqlExecutionOptimalPlan implements OptimalPlan {
  private final List<Subplan> subplans;
  SqlExecutionOptimalPlan(List<Subplan> subplans){
    this.subplans = subplans;
  }

  public SqlExecutionOptimalPlan(){
    this(Lists.<Subplan>newArrayList());
  }

  @Override public boolean addSubplan(Subplan subplan) {
    return !subplans.contains(subplan) && subplans.add(subplan);
  }

  @Override public void computeInternalPlanCost() {
    //tocode
  }

  @Override public double getTotalCost() {
    return 0;  //tocode
  }

  @Override public double getAccessCost(String tableName) {
    return 0;  //tocode
  }

  @Override public double getInternalCost() {
    return 0;  //tocode
  }

  @Override public boolean isDirty() {
    return false;  //tocode
  }

  @Override public boolean removeSubplan(Subplan subplan) {
    return subplans.contains(subplan) && subplans.remove(subplan);
  }

  @Override public void setAccessCost(String tableName, double cost) {
    //tocode
  }

  /**
   * Default implementation of {@link Subplan} interface.
   */
  public static class InternalSubplan implements Subplan {
    private final int     rowId;
    private final int     parentId;
    private final String  target;
    private final double  cost;
    private final long    cardinality;
    private final double  initCost;
    private final String operator;

    public InternalSubplan(int rowId, int parentId,
        String operator, String target, double cost, double initCost, long cardinality){
      this.rowId        = rowId;
      this.parentId     = parentId;
      this.target       = target;
      this.operator     = operator;
      this.cost         = cost;
      this.initCost     = initCost;
      this.cardinality  = cardinality;
    }

    @Override public int getRowId() {
      return rowId;
    }

    @Override public int getParentId() {
      return parentId;
    }

    @Override public String getOperator() {
      return operator;
    }

    @Override public String getTarget() {
      return target;
    }

    @Override public double getCost() {
      return cost;
    }

    @Override public double getInitCost() {
      return initCost;
    }

    @Override public long getCardinality() {
      return cardinality;
    }

    @Override public String toString() {
      return Objects.toStringHelper(this)
          .add("rowId", getRowId())
          .add("parentId", getParentId())
          .add("target", getTarget())
          .add("operator", getOperator())
          .add("wholeOperationCost", getCost())
          .add("initCost", getInitCost())
          .add("cardinality", getCardinality())
      .toString();
    }
  }
}
