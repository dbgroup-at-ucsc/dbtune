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
  private final Map<DBIndex, Set<OptimalPlan>> cachedPlans;
  InMemoryInumSpace(Map<DBIndex, Set<OptimalPlan>> cachedPlans){
    this.cachedPlans = cachedPlans;
  }

  public InMemoryInumSpace(){
    this(Maps.<DBIndex, Set<OptimalPlan>>newHashMap());
  }

  @Override public void clear() {
    synchronized (cachedPlans){
      cachedPlans.clear();
    }
  }

  @Override public Set<OptimalPlan> getOptimalPlans(DBIndex key) {
    return cachedPlans.get(key);
  }

  @Override public Set<OptimalPlan> getAllSavedOptimalPlans() {
    final Set<OptimalPlan> allSavedOnes = Sets.newHashSet();
    for(Set<OptimalPlan> each : cachedPlans.values()){
      allSavedOnes.addAll(each);
    }
    return ImmutableSet.copyOf(allSavedOnes);
  }

  @Override public Set<OptimalPlan> save(DBIndex interestingOrder, Set<OptimalPlan> optimalPlans) {
    if(!cachedPlans.containsKey(interestingOrder)){
      final Set<OptimalPlan> newBatchedOfPlans = Sets.newHashSet();
      for(OptimalPlan each : optimalPlans){
        newBatchedOfPlans.add(new CachedSqlExecutionOptimalPlan(each));
      }

      cachedPlans.put(interestingOrder, newBatchedOfPlans);
    } else {
      for(OptimalPlan each : optimalPlans){
        final CachedSqlExecutionOptimalPlan cp = new CachedSqlExecutionOptimalPlan(each);
        if(!cachedPlans.get(interestingOrder).contains(cp)){
           cachedPlans.get(interestingOrder).add(cp);
        }
      }
    }

    return getOptimalPlans(interestingOrder);
  }

  @Override public String toString() {
    return Objects.toStringHelper(this)
        .add("cached plans", getAllSavedOptimalPlans())
    .toString();
  }
}
