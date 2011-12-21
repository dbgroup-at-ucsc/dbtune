package edu.ucsc.dbtune.inum;

import com.google.caliper.internal.guava.collect.Maps;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import edu.ucsc.dbtune.metadata.Configuration;
import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.util.Strings;
import static java.lang.Double.compare;
import java.util.List;
import java.util.Map;

/**
 * The not yet cached Sql Execution Plan.
 *
 * @author hsanchez@cs.ucsc.edu (Huascar A. Sanchez)
 */
public abstract class SqlExecutionOptimalPlan implements OptimalPlan {
  protected final List<PhysicalOperator> internalPlans;
  protected final Map<String, Double>    tableNameToAccessCost;

  protected double            internal;
  private   PhysicalOperator  root;

  SqlExecutionOptimalPlan(List<PhysicalOperator> internalPlans) {
    this.internalPlans          = internalPlans;
    this.internal               = Double.NaN;
    this.tableNameToAccessCost  = Maps.newHashMap();
    this.root                   = null;
  }

  protected SqlExecutionOptimalPlan() {
    this(Lists.<PhysicalOperator>newArrayList());
  }

  @Override public boolean add(PhysicalOperator operator) {
    return !internalPlans.contains(operator) && internalPlans.add(operator);
  }

  @Override public abstract void computeInternalPlanCost();

  @Override public void fixAccessCosts(Configuration configuration, QueryRecord queryRecord) {
    double internal = getInternalCost();
    for (String eachTableName : queryRecord.getUsedTablesNames()){
      if(compare(getAccessCost(eachTableName.toUpperCase()), 0) <= 0/*check if second is less than first*/) {
        // check if there is an index on it.
        final Index idx = firstIndexForTable(eachTableName.toUpperCase(), configuration);
        if(idx != null && idx.isMaterialized()){
          double indexAccessCost = getAccessCost(idx.getName() /*should be idx's implemented-name*/);
          if(compare(indexAccessCost, 0) >= 0){
            setAccessCost(eachTableName.toUpperCase(), indexAccessCost);
          }
        }
      }

      internal -= getAccessCost(eachTableName.toUpperCase());
    }

    setInternalCost(internal); // note: this was not included in the old implementation; however,
    // from the old code, we can deduce that this previous method call was needed.
  }

  private static Index firstIndexForTable(String tablename, Configuration configuration){
    for(Index each : configuration.toList()){
      final String eachTablename = each.getTable().getName();
      if(Strings.same(eachTablename, tablename)){
        return each;
      }
    }
    return null; // null is allowed
  }

  @Override public List<PhysicalOperator> getInternalPlans() {
    return ImmutableList.copyOf(internalPlans);
  }

  @Override public double getAccessCost(String tableName) {
    final String nonNullTablename = Preconditions.checkNotNull(tableName);
    Double cost = tableNameToAccessCost.get(nonNullTablename);
    final boolean nonNullCost = cost != null;

    return nonNullCost ? cost
        : ((cost = getAccessCost(nonNullTablename.toUpperCase())) != null ? cost : 0);
  }

  @Override public double getInternalCost() {
    return internal;
  }

  public PhysicalOperator getRoot(){
    return root;
  }

  @Override public double getTotalCost() {
    return Preconditions.checkNotNull(getRoot()).getCost();
  }

  @Override public boolean isDirty() {
    return false;
  }

  @Override public boolean remove(PhysicalOperator operator) {
    return internalPlans.contains(operator) && internalPlans.remove(operator);
  }

  @Override public void setAccessCost(String tableName, double cost) {
    final String nonNullTablename = Preconditions.checkNotNull(tableName);
    tableNameToAccessCost.put(nonNullTablename.toUpperCase(), cost);
  }

  void setInternalCost(double cost){
    this.internal = cost;
  }

  public void setRoot(PhysicalOperator operator){
    this.root = Preconditions.checkNotNull(operator);
  }

  @Override public String toString() {
    return internalPlans.toString();
  }
}
