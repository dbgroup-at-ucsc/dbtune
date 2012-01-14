package edu.ucsc.dbtune.inum;

import edu.ucsc.dbtune.util.ConfigurationUtils;
import edu.ucsc.dbtune.util.Strings;
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
  private final Map<QueryRecord, Set<OptimalPlan>> keyToOptimalPlans;

  /**
   * Construct a new {@code InMemoryInumSpace} object given a set of cached
   * plans.
   * @param cachedPlans
   *    a set of cached plans.
   */
  InMemoryInumSpace(Map<QueryRecord, Set<OptimalPlan>> cachedPlans){
    this.keyToOptimalPlans = cachedPlans;
  }

  /**
   * Construct a new {@code InMemoryInumSpace} object given an empty set of cached
   * plans.
   * @see #InMemoryInumSpace(Map)
   */
  public InMemoryInumSpace(){
    this(Maps.<QueryRecord, Set<OptimalPlan>>newHashMap());
  }

  @Override public void clear() {
    synchronized (keyToOptimalPlans){
      keyToOptimalPlans.clear();
    }
  }

  @Override public Set<OptimalPlan> getOptimalPlans(QueryRecord targetKey) {
    final Set<OptimalPlan> found = Sets.newHashSet();
    for(QueryRecord eachKey : keySet()){
      if(Objects.equal(eachKey, targetKey)){
        found.addAll(keyToOptimalPlans.get(targetKey));
      } else {
        final boolean isSameSql = Strings.same(eachKey.getSQL(), targetKey.getSQL());
        final boolean covered   = ConfigurationUtils.isAcoveringB(targetKey.getConfiguration(), eachKey.getConfiguration());
        if(isSameSql && covered){
          found.addAll(keyToOptimalPlans.get(eachKey));
        }
      }
    }

    return found;
  }

  @Override public Set<QueryRecord> keySet() {
    return ImmutableSet.copyOf(keyToOptimalPlans.keySet());
  }

  @Override public Set<OptimalPlan> getAllSavedOptimalPlans() {
    final Set<OptimalPlan> allSavedOnes = Sets.newHashSet();
    for(Set<OptimalPlan> each : keyToOptimalPlans.values()){
      allSavedOnes.addAll(each);
    }
    return ImmutableSet.copyOf(allSavedOnes);
  }

  @Override public InumSpace save(QueryRecord key, Set<OptimalPlan> optimalPlans) {
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
  
  /**
   * Return the set of template plans   
   * 
   * @return 
   * 	The set of template plans
   */
  @Override public Set<InumStatementPlan> getTemplatePlans(){
	  throw new RuntimeException("NOT IMPLEMENTED YET");
  }
}
