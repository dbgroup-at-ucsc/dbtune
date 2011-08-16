package edu.ucsc.dbtune.inum;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import edu.ucsc.dbtune.core.DBIndex;
import edu.ucsc.dbtune.core.DatabaseConnection;
import edu.ucsc.dbtune.spi.Environment;
import edu.ucsc.dbtune.spi.core.Console;
import edu.ucsc.dbtune.util.StopWatch;
import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
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

  private static final Set<String> WORKLOADS;
  static {
    final Environment environment   = Environment.getInstance();
    final String      workloadPath  = environment.getScriptAtWorkloadsFolder("inum/");

    final SetupWorkloadVisitor  loader            = new SetupWorkloadVisitor();
    final WorkloadDirectoryNode workloadDirectory = new WorkloadDirectoryNode(new File(workloadPath));
    WORKLOADS = ImmutableSet.copyOf(workloadDirectory.accept(loader));
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
        new InumMatchingStrategy()
    );
  }

  public InumSpace getInumSpace(){
    return precomputation.getInumSpace();
  }

  /**
   * INUM setup will load any representative workload found in the inum workload
   * directory.
   */
  public void start(){
    try {
      start(WORKLOADS);
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
      // todo(Huascar) question to Team: should we parse the interesting orders or rely on
      //           the recommended indexes by the extractor? For sake of speed, I am
      //           using the recommended indexes. For parsing the interesting orders, we
      //           need using Zql parser (include in Dash's code.
      final Iterable<DBIndex> ios = recommendPromissingIndexes(eachQuery, connection);
      precomputation.setup(eachQuery, ios);
    }
    timing.resetAndLog("precomputation took ");
  }

  private static Iterable<DBIndex> recommendPromissingIndexes(String query,
      DatabaseConnection connection){
    try {
      return connection.getIndexExtractor().recommendIndexes(query);
    } catch (SQLException e) {
      Console.streaming().error("unable to get indexes. an empty set is returned.", e);
      return ImmutableSet.of();
    }
  }


  public double calculateQueryCost(String workload, Iterable<DBIndex> inputConfiguration){
    if(!isStarted.get()) throw new InumExecutionException("INUM has not been started yet. Please call start(..) method.");
    if(!precomputation.skip(workload)) {
      precomputation.setup(workload, inputConfiguration);
    }

    final InumSpace   cachedPlans       = precomputation.getInumSpace();
    final OptimalPlan singleOptimalPlan = matchingLogic.matches(
        cachedPlans.getAllSavedOptimalPlans(),
        inputConfiguration
    );

    return matchingLogic.derivesCost(singleOptimalPlan, inputConfiguration);
  }

  public void end()   {
    isStarted.set(false);
    precomputation.getInumSpace().clear();
  }
}
