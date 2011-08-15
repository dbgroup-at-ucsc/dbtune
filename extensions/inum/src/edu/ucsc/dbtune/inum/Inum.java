package edu.ucsc.dbtune.inum;

import com.google.common.collect.Lists;
import edu.ucsc.dbtune.core.DBIndex;
import edu.ucsc.dbtune.inum.commons.Pair;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * It serves as the entry point to the INUM functionality. It will
 * orchestrate the execution of INUM.
 *
 * @author hsanchez@cs.ucsc.edu (Huascar A. Sanchez)
 */
public class Inum {
  private final Precomputation    precomputation;
  private final MatchingStrategy  matchingLogic;
  private final AtomicBoolean     isStarted;

  private Inum(Precomputation precomputation, MatchingStrategy matchingLogic){
    this.precomputation = precomputation;
    this.matchingLogic  = matchingLogic;
    this.isStarted      = new AtomicBoolean(false);
  }

  public static Inum newInumInstance(Precomputation precomputation, MatchingStrategy matchingLogic){
    return new Inum(precomputation, matchingLogic);
  }

  /**
   * INUM setup will be on demand.
   */
  public void start(){
    start(Lists.<Pair<String,Iterable<DBIndex>>>newArrayList());
  }

  /**
   * INUM will get prepopulated first with representative workloads and configurations.
   * @param input
   *    a list of <query-configuration> pairs.
   */
  public void start(List<Pair<String, Iterable<DBIndex>>> input) {
    isStarted.set(true);
    for(Pair<String,       // the query
        Iterable<DBIndex>  // the configuration
        > each : input){

      precomputation.setup(each.getLeft(), each.getRight());
    }
  }

  public double calculateQueryCost(String workload, Iterable<DBIndex> inputConfiguration){
    if(!isStarted.get()) throw new InumExecutionException("INUM has not been started yet. Please call start(..) method.");
    if(!precomputation.skip(workload)) {
      precomputation.setup(workload, inputConfiguration);
    }

    final InumSpace cachedPlans = precomputation.getInumSpace();
    final OptimalPlan singleOptimalPlan = matchingLogic.matches(
        cachedPlans.getAllSavedOptimalPlans(),
        inputConfiguration
    );

    return matchingLogic.derivesCost(singleOptimalPlan, inputConfiguration);
  }

  public void end()   {
    isStarted.set(false);
  }
}
