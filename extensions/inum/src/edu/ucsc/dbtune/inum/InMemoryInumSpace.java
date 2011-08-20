package edu.ucsc.dbtune.inum;

import com.google.caliper.internal.guava.collect.Maps;
import com.google.common.base.Objects;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import edu.ucsc.dbtune.core.DBIndex;
import java.util.Map;
import java.util.Set;

/**
 * An in-memory inum space. In-memory means that you will have
 * access to the cached plans only during the execution of the Inum program.
 * Once stopping Inum, all saved plans will be garbaged collected.
 *
 * @author hsanchez@cs.ucsc.edu (Huascar A. Sanchez)
 */
public class InMemoryInumSpace implements InumSpace {
  private final Map<Set<DBIndex>, Set<OptimalPlan>> cachedPlans;
  InMemoryInumSpace(Map<Set<DBIndex>, Set<OptimalPlan>> cachedPlans){
    this.cachedPlans = cachedPlans;
  }

  public InMemoryInumSpace(){
    this(Maps.<Set<DBIndex>, Set<OptimalPlan>>newHashMap());
  }

  @Override public void clear() {
    synchronized (cachedPlans){
      cachedPlans.clear();
    }
  }

  @Override public Set<OptimalPlan> getOptimalPlans(Set<DBIndex> key) {
    return cachedPlans.get(key);
  }

  @Override public Set<OptimalPlan> getAllSavedOptimalPlans() {
    final Set<OptimalPlan> allSavedOnes = Sets.newHashSet();
    for(Set<OptimalPlan> each : cachedPlans.values()){
      allSavedOnes.addAll(each);
    }
    return ImmutableSet.copyOf(allSavedOnes);
  }

  @Override public Set<OptimalPlan> save(Set<DBIndex> interestingOrders, Set<OptimalPlan> optimalPlans) {
    if(!cachedPlans.containsKey(interestingOrders)){
      final Set<OptimalPlan> newBatchedOfPlans = Sets.newHashSet();
      for(OptimalPlan each : optimalPlans){
        newBatchedOfPlans.add(new CachedSqlExecutionOptimalPlan(each));
      }

      cachedPlans.put(interestingOrders, newBatchedOfPlans);
    } else {
      for(OptimalPlan each : optimalPlans){
        final CachedSqlExecutionOptimalPlan cp = new CachedSqlExecutionOptimalPlan(each);
        if(!cachedPlans.get(interestingOrders).contains(cp)){
           cachedPlans.get(interestingOrders).add(cp);
        }
      }
    }

    return getOptimalPlans(interestingOrders);
  }

  @Override public String toString() {
    return Objects.toStringHelper(this)
        .add("cached plans", getAllSavedOptimalPlans())
    .toString();
  }
}
