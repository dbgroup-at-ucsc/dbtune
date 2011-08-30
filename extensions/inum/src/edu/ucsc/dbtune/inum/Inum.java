package edu.ucsc.dbtune.inum;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import edu.ucsc.dbtune.core.DBIndex;
import edu.ucsc.dbtune.core.DatabaseConnection;
import edu.ucsc.dbtune.spi.core.Console;
import edu.ucsc.dbtune.util.Combinations;
import edu.ucsc.dbtune.util.StopWatch;
import java.io.IOException;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * It serves as the entry point to the INUM functionality. It will
 * orchestrate the execution of INUM.
 *
 * @author hsanchez@cs.ucsc.edu (Huascar A. Sanchez)
 */
public class Inum {
  private final DatabaseConnection connection;
  private final Precomputation     precomputation;
  private final MatchingStrategy   matchingLogic;
  private final AtomicBoolean      isStarted;

  private static final Set<String> QUERIES;
  static {
    final SetupWorkloadVisitor  loader            = new SetupWorkloadVisitor();
    final WorkloadDirectoryNode workloadDirectory = new WorkloadDirectoryNode();
    QUERIES = ImmutableSet.copyOf(workloadDirectory.accept(loader));
  }

  private Inum(DatabaseConnection connection, Precomputation precomputation, MatchingStrategy matchingLogic){
    this.connection     = connection;
    this.precomputation = precomputation;
    this.matchingLogic  = matchingLogic;
    this.isStarted      = new AtomicBoolean(false);
  }

  public static Inum newInumInstance(DatabaseConnection connection,
      Precomputation precomputation,
      MatchingStrategy matchingLogic){
    final DatabaseConnection nonNullConnection     = Preconditions.checkNotNull(connection);
    final Precomputation     nonNullPrecomputation = Preconditions.checkNotNull(precomputation);
    final MatchingStrategy   nonNullMatchingLogic  = Preconditions.checkNotNull(matchingLogic);
    return new Inum(nonNullConnection, nonNullPrecomputation, nonNullMatchingLogic);
  }

  public static Inum newInumInstance(DatabaseConnection connection){
    final DatabaseConnection nonNullConnection = Preconditions.checkNotNull(connection);
    return newInumInstance(
        nonNullConnection,
        new InumPrecomputation(nonNullConnection),
        new InumMatchingStrategy(nonNullConnection)
    );
  }

  public double estimateCost(String query, Iterable<DBIndex> inputConfiguration){
    final String errmsg = "INUM has not been started yet. Please call start(..) method.";
    if(!isStarted.get()) throw new InumExecutionException(errmsg);
    if(!precomputation.skip(query)) {
      precomputation.setup(
          query, 
          findInterestingOrders(query, connection)
      );
    }

    final InumSpace   cachedPlans       = precomputation.getInumSpace();
    final OptimalPlan singleOptimalPlan = matchingLogic.matches(
        cachedPlans,
        inputConfiguration
    );

    return matchingLogic.derivesCost(query, singleOptimalPlan, inputConfiguration);
  }

  public void end()   {
    isStarted.set(false);
    precomputation.getInumSpace().clear();
  }
  
  public Iterable<DBIndex> findInterestingOrders(String query,
      DatabaseConnection connection){
    return InterestingOrders.extractInterestingOrders(query, connection);
  }

  public InumSpace getInumSpace(){
    return precomputation.getInumSpace();
  }
  
  public DatabaseConnection getDatabaseConnection(){
    return connection;
  }

  /**
   * INUM setup will load any representative workload found in the inum workload
   * directory.
   */
  public void start(){
    try {
      start(QUERIES);
    } catch (IOException e) {
      Console.streaming().error("unable to load workload", e);
    }
  }

  /**
   * INUM will get prepopulated first with representative workloads and configurations.
   * @param input
   *    a list of representative workloads.
   * @throws IOException
   *    if unable to parse the input.
   */
  public void start(Set<String> input) throws IOException {
    isStarted.set(true);

    final StopWatch timing = new StopWatch();
    for(String eachQuery : input){
      precomputation.setup(eachQuery, findInterestingOrders(eachQuery, connection));
    }
    timing.resetAndLog("precomputation took ");
  }

  @Override public String toString() {
    return Objects.toStringHelper(this)
        .add("started?", isStarted.get())
        .add("liveDBConnection?", getDatabaseConnection().isOpened())
    .toString();
  }
}
