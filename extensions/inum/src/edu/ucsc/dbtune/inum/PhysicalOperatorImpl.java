package edu.ucsc.dbtune.inum;

import com.google.common.base.Objects;

/**
 * Default implementation of {@link PhysicalOperator} interface.
 *
 * @author hsanchez@cs.ucsc.edu (Huascar A. Sanchez)
 */
public class PhysicalOperatorImpl implements PhysicalOperator {
  private final int rowId;
  private final int parentId;
  private final String target;
  private final double cost;
  private final long cardinality;
  private final double initCost;
  private final String operator;

  public PhysicalOperatorImpl(int rowId, int parentId,
      String operator, String target, double cost, double initCost, long cardinality) {
    this.rowId = rowId;
    this.parentId = parentId;
    this.target = target;
    this.operator = operator;
    this.cost = cost;
    this.initCost = initCost;
    this.cardinality = cardinality;
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
