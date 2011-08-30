package edu.ucsc.dbtune.core;

import com.google.common.collect.Sets;
import edu.ucsc.dbtune.inum.Inum;

/**
 * default implementation of {@link InumWhatIfOptimizer} type.
 *
 * @author hsanchez@cs.ucsc.edu (Huascar A. Sanchez)
 */
public class InumWhatIfOptimizerImpl implements InumWhatIfOptimizer {
  private final Inum                                inum;


  /**
   * construct a new {@code InumWhatIfOptimizer} object.
   * @param connection
   *    a live database connection to postgres
   */
  InumWhatIfOptimizerImpl(DatabaseConnection connection){
    this.inum       = Inum.newInumInstance(connection);
    this.inum.start();
  }

  @Override public double estimateCost(String workload) {
    return estimateCost(workload, Sets.<DBIndex>newHashSet());
  }

  @Override public double estimateCost(String workload, Iterable<DBIndex> hypotheticalIndexes) {
    return inum.estimateCost(workload, hypotheticalIndexes);
  }

  /**
   * start inum
   */
  public void startInum(){
    inum.start();
  }

  /**
   * shut down inum.
   */
  public void shutdownInum(){
    inum.end();
  }

  @Override public String toString() {
    return String.format("Inum status = %s" + inum.toString());
  }
}
