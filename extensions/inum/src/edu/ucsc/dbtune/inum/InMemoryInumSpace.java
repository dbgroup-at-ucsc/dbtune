package edu.ucsc.dbtune.inum;

import java.util.Map;
import java.util.Set;

import com.google.common.collect.Maps;
import com.google.common.base.Objects;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

/**
 * An in-memory inum space. In-memory means that you will have
 * access to the cached plans only during the execution of the Inum program.
 * Once stopping Inum, all saved plans will be garbaged collected.
 *
 * @author hsanchez@cs.ucsc.edu (Huascar A. Sanchez)
 */
public class InMemoryInumSpace implements InumSpace {
  private final Map<Key, Set<OptimalPlan>> keyToOptimalPlans;

  InMemoryInumSpace(Map<Key, Set<OptimalPlan>> cachedPlans){
    this.keyToOptimalPlans = cachedPlans;
  }

  public InMemoryInumSpace(){
    this(Maps.<Key, Set<OptimalPlan>>newHashMap());
  }

  @Override public void clear() {
    synchronized (keyToOptimalPlans){
      keyToOptimalPlans.clear();
    }
  }

  @Override public Set<OptimalPlan> getOptimalPlans(Key key) {
    return keyToOptimalPlans.get(key);
  }

  @Override public Set<Key> keySet() {
    return ImmutableSet.copyOf(keyToOptimalPlans.keySet());
  }

  @Override public Set<OptimalPlan> getAllSavedOptimalPlans() {
    final Set<OptimalPlan> allSavedOnes = Sets.newHashSet();
    for(Set<OptimalPlan> each : keyToOptimalPlans.values()){
      allSavedOnes.addAll(each);
    }
    return ImmutableSet.copyOf(allSavedOnes);
  }

  @Override public InumSpace save(Key key, Set<OptimalPlan> optimalPlans) {
    if(!keyToOptimalPlans.containsKey(key)){
      final Set<OptimalPlan> newBatchedOfPlans = Sets.newHashSet();
      for(OptimalPlan each : optimalPlans){
        newBatchedOfPlans.add(new CachedSqlExecutionOptimalPlan(each));
      }

      keyToOptimalPlans.put(key, newBatchedOfPlans);
    } else {
      for(OptimalPlan each : optimalPlans){
        final CachedSqlExecutionOptimalPlan cp = new CachedSqlExecutionOptimalPlan(each);
        if(!keyToOptimalPlans.get(key).contains(cp)){
           keyToOptimalPlans.get(key).add(cp);
        }
      }
    }

    return this;
  }

  @Override public String toString() {
    return Objects.toStringHelper(this)
        .add("cached plans", getAllSavedOptimalPlans())
    .toString();
  }
}
