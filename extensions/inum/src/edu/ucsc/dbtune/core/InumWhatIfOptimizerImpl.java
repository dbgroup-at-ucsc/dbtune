package edu.ucsc.dbtune.core;

import com.google.common.base.Preconditions;
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
  public InumWhatIfOptimizerImpl(DatabaseConnection connection){
    this(Inum.newInumInstance(Preconditions.checkNotNull(connection)));
  }

  /**
   * construct a new {@code InumWhatIfOptimizer} object.
   * @param inum
   *    a new instance of INUM.
   */
  public InumWhatIfOptimizerImpl(Inum inum){
    this.inum = inum;
  }


  @Override public double estimateCost(String workload) {
    if(inum.isEnded()) { startInum(); }
    return estimateCost(workload, Sets.<DBIndex>newHashSet());
  }

  @Override public double estimateCost(String workload, Iterable<DBIndex> hypotheticalIndexes) {
    if(inum.isEnded()) { startInum(); }
    return inum.estimateCost(workload, hypotheticalIndexes);
  }

  /**
   * start INUM.
   */
  void startInum(){
    inum.start();
  }

  /**
   * shut down INUM. This method may be called when the main component of the dbtune API
   * request a closing of their {@link DatabaseConnection} object. Once this db connection is
   * closed, there is not need to keep INUM 'on' too.
   */
  void endInum(){
    inum.end();
  }

  @Override public String toString() {
    return String.format("Inum status = %s" + inum.toString());
  }
}
