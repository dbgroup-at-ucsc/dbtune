package edu.ucsc.dbtune.inum;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import edu.ucsc.dbtune.metadata.Catalog;
import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.util.StopWatch;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * This object represents the entry point to the INUM functionality. It will
 * orchestrate the execution of INUM.
 *
 * @author hsanchez@cs.ucsc.edu (Huascar A. Sanchez)
 */
public class Inum {
  private final Connection connection;
  private final Precomputation precomputation;
  private final MatchingStrategy matchingLogic;
  private final InterestingOrdersExtractor ioExtractor;
  private final AtomicBoolean isStarted;

  private static final Set<String> QUERIES;

  static {
    final SetupWorkloadVisitor loader = new SetupWorkloadVisitor();
    final WorkloadDirectoryNode workloadDirectory = new WorkloadDirectoryNode();
    QUERIES = ImmutableSet.copyOf(workloadDirectory.accept(loader));
  }

  /**
   * Construct a new instance of {@code INUM} given a dbms connection, a precomputation strategy,
   * a matching logic, and an interesting orders extractor.
   * @param connection
   *    a live dbms connection.
   * @param precomputation
   *    a strategy precomputation step populates the INUM Space at *initialization time*.
   * @param matchingLogic
   *    a matching logic that determines the optimality of the reused plans.
   * @param extractor
   *    a strategy object that extracts interesting orders from a given sql query.
   */
  private Inum(
      Connection connection,
      Precomputation precomputation,
      MatchingStrategy matchingLogic,
      InterestingOrdersExtractor extractor) {
    this.connection = connection;
    this.precomputation = precomputation;
    this.matchingLogic = matchingLogic;
    this.ioExtractor = extractor;
    this.isStarted = new AtomicBoolean(false);
  }

  /**
   * @see #Inum(Connection, Precomputation, MatchingStrategy, InterestingOrdersExtractor)
   */
  public static Inum newInumInstance(
      Connection connection,
      Precomputation precomputation,
      MatchingStrategy matchingLogic,
      InterestingOrdersExtractor extractor) {
    final Connection nonNullConnection = Preconditions.checkNotNull(connection);
    final Precomputation nonNullPrecomputation = Preconditions.checkNotNull(precomputation);
    final MatchingStrategy nonNullMatchingLogic = Preconditions.checkNotNull(matchingLogic);
    final InterestingOrdersExtractor nonNullInteresingOrdersExtractor = Preconditions
        .checkNotNull(extractor);

    return new Inum(nonNullConnection, nonNullPrecomputation,
        nonNullMatchingLogic, nonNullInteresingOrdersExtractor);
  }

  /**
   * Construct an {@code INUM} object given a catalog and a live dbms connection.
   * @param catalog
   *    a container of schema objects.
   * @param connection
   *    a live dbms connection.
   * @return a new {@code INUM} object.
   * @see #Inum(Connection, Precomputation, MatchingStrategy, InterestingOrdersExtractor)
   */
  public static Inum newInumInstance(Catalog catalog, Connection connection) {
    final Connection nonNullConnection = Preconditions.checkNotNull(connection);
    final Catalog nonNullCatalog = Preconditions.checkNotNull(catalog);
    return newInumInstance(
        nonNullConnection,
        new InumPrecomputation(nonNullConnection),
        new InumMatchingStrategy(),
        new InumInterestingOrdersExtractor(nonNullCatalog, nonNullConnection));
  }

  /**
   * derives the query cost without going to the optimizer.
   * @param query
   *    single query.
   * @param inputConfiguration
   *    the input configuration be evaluated.
   * @return
   *    the query cost computed on-the-fly.
   * @throws SQLException
   *    unable to estimate cost due to the stated reasons.
   */
  public double estimateCost(String query, Set<Index> inputConfiguration)
      throws SQLException {
    final String errmsg = "INUM has not been started yet. Please call start(..) method.";

    if (isEnded()) { throw new InumExecutionException(errmsg); }

    if (!precomputation.skip(query)) {
      precomputation.setup(query, findInterestingOrders(query));
    }

    return matchingLogic.estimateCost(query, inputConfiguration, precomputation.getInumSpace());
  }

  /**
   * it shuts down INUM.
   */
  public void end() {
    isStarted.set(false);
    precomputation.getInumSpace().clear();
  }

  /**
   * find the interesting orders defined in a single query.
   * @param query
   *    single query.
   * @return a set of interesting orders.
   * @throws SQLException
   *    unable to find these interesting orders due to the stated reasons.
   */
  public List<Set<Index>> findInterestingOrders(String query)
      throws SQLException {
    return ioExtractor.extractInterestingOrders(query);
  }

  /**
   * @return the current {@link InumSpace}.
   */
  public InumSpace getInumSpace() {
    return precomputation.getInumSpace();
  }

  /**
   * @return a live dbms connection.
   */
  public Connection getConnection() {
    return connection;
  }

  /**
   * @return {@code true} if INUM has stopped running. {@code false} otherwise.
   */
  public boolean isEnded() {
    return !isStarted();
  }

  /**
   * @return {@code true} if INUM has started running. {@code false} otherwise.
   */
  public boolean isStarted() {
    return isStarted.get();
  }

  /**
   * INUM setup will load any representative workload found in the inum workload directory.
   *
   * @throws SQLException if unable to build the inum space.
   */
  public void start() throws SQLException {
    start(QUERIES);
  }

  /**
   * INUM will get prepopulated first with representative workloads and configurations.
   *
   * @param input a list of representative workloads.
   * @throws SQLException if unable to build inum space.
   */
  public void start(Set<String> input) throws SQLException {
    isStarted.set(true);

    final StopWatch timing = new StopWatch();

    for (String eachQuery : input) {
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
