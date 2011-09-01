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

  /**
   * shut down INUM. This method may be called when the main component of the dbtune API
   * request a closing of their {@link DatabaseConnection} object. Once this db connection is
   * closed, there is not need to keep INUM 'on' too.
   */
  public void endInum(){
    if(getInum().isStarted()) getInum().end();
  }


  @Override public double estimateCost(String workload) {
    if(getInum().isEnded()) { startInum(); }
    return estimateCost(workload, Sets.<DBIndex>newHashSet());
  }

  @Override public double estimateCost(String workload, Iterable<DBIndex> hypotheticalIndexes) {
    if(getInum().isEnded()) { startInum(); }
    return getInum().estimateCost(workload, hypotheticalIndexes);
  }

  /**
   * @return the assigned {@link Inum} object.
   */
  public Inum getInum(){
    return inum;
  }

  /**
   * start INUM.
   */
  public void startInum(){
    getInum().start();
  }

  @Override public String toString() {
    return String.format("Inum status = %s" + getInum().toString());
  }
}
