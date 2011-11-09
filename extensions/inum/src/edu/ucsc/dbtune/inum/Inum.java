package edu.ucsc.dbtune.inum;

import edu.ucsc.dbtune.metadata.Configuration;
import edu.ucsc.dbtune.metadata.Catalog;
import edu.ucsc.dbtune.spi.Console;
import edu.ucsc.dbtune.util.StopWatch;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;

import java.io.IOException;
import java.sql.Connection;
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
  private final Connection                 connection;
  private final Precomputation             precomputation;
  private final MatchingStrategy           matchingLogic;
  private final InterestingOrdersExtractor ioExtractor;
  private final AtomicBoolean              isStarted;
  private final Catalog                    catalog;

  private static final Set<String> QUERIES;
  static {
    final SetupWorkloadVisitor  loader            = new SetupWorkloadVisitor();
    final WorkloadDirectoryNode workloadDirectory = new WorkloadDirectoryNode();
    QUERIES = ImmutableSet.copyOf(workloadDirectory.accept(loader));
  }

  private Inum(
          Catalog catalog,
          Connection connection,
          Precomputation precomputation,
          MatchingStrategy matchingLogic,
          InterestingOrdersExtractor extractor)
  {
    this.catalog        = catalog;
    this.connection     = connection;
    this.precomputation = precomputation;
    this.matchingLogic  = matchingLogic;
    this.ioExtractor    = extractor;
    this.isStarted      = new AtomicBoolean(false);
  }

  public static Inum newInumInstance(
          Catalog catalog,
          Connection connection,
          Precomputation precomputation,
          MatchingStrategy matchingLogic,
          InterestingOrdersExtractor extractor)
  {
    final Connection                 nonNullConnection                = Preconditions.checkNotNull(connection);
    final Precomputation             nonNullPrecomputation            = Preconditions.checkNotNull(precomputation);
    final MatchingStrategy           nonNullMatchingLogic             = Preconditions.checkNotNull(matchingLogic);
    final InterestingOrdersExtractor nonNullInteresingOrdersExtractor = Preconditions.checkNotNull(extractor);

    return new Inum(catalog, nonNullConnection, nonNullPrecomputation, 
            nonNullMatchingLogic, nonNullInteresingOrdersExtractor);
  }

  public static Inum newInumInstance(Catalog catalog, Connection connection){
    final Connection nonNullConnection = Preconditions.checkNotNull(connection);
    return newInumInstance(
        catalog,
        nonNullConnection,
        new InumPrecomputation(nonNullConnection),
        new InumMatchingStrategy(nonNullConnection),
        new InumInterestingOrdersExtractor(catalog, nonNullConnection));
  }

  public double estimateCost(String query, Configuration inputConfiguration) 
      throws SQLException {
    final String errmsg = "INUM has not been started yet. Please call start(..) method.";
    if(isEnded()) throw new InumExecutionException(errmsg);
    if(!precomputation.skip(query)) {
      precomputation.setup(
          query, 
          findInterestingOrders(query)
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
  
  public Configuration findInterestingOrders(String query) throws SQLException{
    return ioExtractor.extractInterestingOrders(query);
  }


  public InumSpace getInumSpace(){
    return precomputation.getInumSpace();
  }
  
  public Connection getConnection(){
    return connection;
  }
  
  public boolean isEnded(){
    return !isStarted();
  }
  
  public boolean isStarted(){
    return isStarted.get();
  }

  /**
   * INUM setup will load any representative workload found in the inum workload
   * directory.
   */
  public void start() throws SQLException {
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
  public void start(Set<String> input) throws IOException,SQLException {
    isStarted.set(true);

    final StopWatch timing = new StopWatch();
    for(String eachQuery : input){
      precomputation.setup(eachQuery, findInterestingOrders(eachQuery));
    }
    timing.resetAndLog("precomputation took ");
  }

  @Override public String toString() {
      
    try {
    return Objects.toStringHelper(this)
        .add("started?", isStarted() ? "Yes" : "No")
        .add("opened DB connection?", getConnection().isClosed() ? "Yes" : "No")
    .toString();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
