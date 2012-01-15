package edu.ucsc.dbtune.inum;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import edu.ucsc.dbtune.util.ConfigurationUtils;
import edu.ucsc.dbtune.util.Strings;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

/**
 * An in-memory and thread safe inum space. In-memory means that you will have
 * access to the cached plans only during the execution of the Inum program.
 * Once stopping Inum, all saved plans will be garbaged collected.
 *
 * @author hsanchez@cs.ucsc.edu (Huascar A. Sanchez)
 */
public class InMemoryInumSpace implements InumSpace {
  // guarded by "this"
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
    this(Maps.<QueryRecord, Set<OptimalPlan>>newConcurrentMap());
  }

  @Override public void clear() {
    synchronized (this){
      keyToOptimalPlans.clear();
    }
  }
  
  private boolean contains(QueryRecord key){
    final boolean result;
    synchronized (this){
      result = keyToOptimalPlans.containsKey(key);
    }
    return result;
  }

  @Override public Set<OptimalPlan> getAllSavedOptimalPlans() {
    final Set<OptimalPlan> allSavedOnes = Sets.newHashSet();
    final Collection<Set<OptimalPlan>> availablePlans;
    synchronized (this){
      availablePlans = keyToOptimalPlans.values();
    }
    for(Set<OptimalPlan> each : availablePlans){
      allSavedOnes.addAll(each);
    }
    return allSavedOnes;
  }

  @Override public Set<OptimalPlan> getOptimalPlans(QueryRecord targetKey) {
    final QueryRecord nonNullTargetKey = Preconditions.checkNotNull(targetKey);
    final Set<OptimalPlan> found = Sets.newHashSet();
    for(QueryRecord eachKey : keySet()){
      if(Objects.equal(eachKey, nonNullTargetKey)){
        found.addAll(getOptimalPlan(nonNullTargetKey));
      } else {
        final boolean isSameSql = Strings.same(eachKey.getSQL(), nonNullTargetKey.getSQL());
        final boolean covered = ConfigurationUtils
            .isAcoveringB(nonNullTargetKey.getConfiguration(), eachKey.getConfiguration());
        if(isSameSql && covered){
          found.addAll(getOptimalPlan(eachKey));
        }
      }
    }

    return found;
  }
  
  private Set<OptimalPlan> getOptimalPlan(QueryRecord key){
    final Set<OptimalPlan> result;
    synchronized (this){
      result = keyToOptimalPlans.get(key);
    }
    return result;
  }
  
  /**
   * Return the set of template plans   
   * 
   * @return 
   * 	The set of template plans
   */
  @Override public Set<InumStatementPlan> getTemplatePlans(){
    throw new UnsupportedOperationException("NOT IMPLEMENTED YET");
  }

  @Override public Set<QueryRecord> keySet() {
    final Set<QueryRecord> result;
    synchronized (this){
      result = keyToOptimalPlans.keySet();
    }
    return result;
  }

  @Override public InumSpace save(QueryRecord key, Set<OptimalPlan> optimalPlans) {
    if(!contains(key)){
      final Set<OptimalPlan> newBatchedOfPlans = Sets.newHashSet();
      for(OptimalPlan each : optimalPlans){
        newBatchedOfPlans.add(new CachedSqlExecutionOptimalPlan(each));
      }

      synchronized (this){
        keyToOptimalPlans.put(key, newBatchedOfPlans);
      }
      
    } else {
      for(OptimalPlan each : optimalPlans){
        final CachedSqlExecutionOptimalPlan cp = new CachedSqlExecutionOptimalPlan(each);
        
        if(!getOptimalPlan(key).contains(cp)){
           getOptimalPlan(key).add(cp);
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
