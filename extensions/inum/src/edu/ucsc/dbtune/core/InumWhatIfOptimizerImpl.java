package edu.ucsc.dbtune.core;

import Zql.ParseException;
import edu.ucsc.dbtune.inum.ConsoleLogger;
import edu.ucsc.dbtune.inum.CostEstimator;
import edu.ucsc.dbtune.inum.InumUtils;
import edu.ucsc.dbtune.inum.PostgresIndexAccessGenerator;
import edu.ucsc.dbtune.inum.autopilot.AutoPilotDelegate;
import edu.ucsc.dbtune.inum.autopilot.autopilot;
import edu.ucsc.dbtune.inum.greedy.GreedyResult;
import edu.ucsc.dbtune.inum.mathprog.CPlex;
import edu.ucsc.dbtune.inum.mathprog.CoPhy;
import edu.ucsc.dbtune.inum.mathprog.CophyConstraints;
import edu.ucsc.dbtune.inum.mathprog.LogListener;
import edu.ucsc.dbtune.inum.model.Configuration;
import edu.ucsc.dbtune.inum.model.Index;
import edu.ucsc.dbtune.inum.model.PhysicalConfiguration;
import edu.ucsc.dbtune.inum.model.Plan;
import edu.ucsc.dbtune.inum.model.WorkloadProcessor;
import edu.ucsc.dbtune.spi.core.Console;
import edu.ucsc.dbtune.util.Checks;
import edu.ucsc.dbtune.util.Instances;
import edu.ucsc.dbtune.util.Iterables;
import edu.ucsc.dbtune.util.StopWatch;
import edu.ucsc.dbtune.util.Strings;
import edu.ucsc.satuning.util.Objects;
import ilog.concert.IloException;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

/**
 * default implementation of {@link InumWhatIfOptimizer} type.
 *
 * @author hsanchez@cs.ucsc.edu (Huascar A. Sanchez)
 */
class InumWhatIfOptimizerImpl implements InumWhatIfOptimizer {
  private final Autopilot                           autopilot;
  private final AtomicReference<GreedyResult>       result;
  private final AtomicReference<WorkloadProcessor>  processor;

  private static final Console CONSOLE = Console.streaming();

  /**
   * construct a new {@code InumWhatIfOptimizer} object.
   * @param connection
   *    a live database connection to postgres
   */
  InumWhatIfOptimizerImpl(DatabaseConnection connection){
    this(new Autopilot(checkDatabaseConnection(connection)),
        Instances.<WorkloadProcessor>newAtomicReference(),
        Instances.<GreedyResult>newAtomicReference()
    );
  }

  /**
   * construct a new {@code InumWhatIfOptimizer} object given a set of queries (workload) and
   * an {@link autopilot autopilot object} for {@code INUM}.
   * @param autopilot
   *    an {@link autopilot autopilot object} required by {@code INUM} to deal with their
   *    physical configurations.
   * @param processor a modified version of inum's {@link WorkloadProcessor}.
   * @param result a result object containing the cost of query given a set of indexes.
   */
  InumWhatIfOptimizerImpl(Autopilot autopilot, AtomicReference<WorkloadProcessor> processor,
      AtomicReference<GreedyResult> result){
    this.autopilot  = autopilot;
    this.result     = result;
    this.processor  = processor;
  }

  private double calculateCost() {
    final GreedyResult current = result.get();
    double total = 0.0;
    for(float each : current.queryCosts){
      total += each;
    }
    return total;
  }

  private static DatabaseConnection checkDatabaseConnection(DatabaseConnection connection){
    if(connection == null) throw new NullPointerException("illegal database connection. reason: connection is null.");
    if(connection.isClosed()) throw new IllegalArgumentException("connection has not been established.");
    return connection;
  }

  @Override public double estimateCost(String workload) {
    return estimateCost(workload, Collections.<DBIndex>emptyList());
  }

  @Override public double estimateCost(String workload, Iterable<DBIndex> hypotheticalIndexes) {
    preprocessIfNotSeenBefore(workload, hypotheticalIndexes);
    try {
      PostgresIndexAccessGenerator.loadIndexAccessCosts(workload, processor.get().query_descriptors);
    } catch (IOException e) {
      CONSOLE.error("unable to load index access costs", e);
    }


    return calculateCost(); // todo(Huascar) is this the correct way to calculate costs? hmm
  }

  private static WorkloadProcessor initializeWorkloadProcessor(String workloadFile,
      Iterable<DBIndex> hypotheticalIndexes)
      throws ParseException {
    // hypothetical indexes are converted into the INUM indexes and set in the
    // workload processor rather than autogenerating indexes.
    final WorkloadProcessorAdapter processor = new WorkloadProcessorAdapter(
        workloadFile,
        hypotheticalIndexes
    );
    processor.useHypotheticalIndexes();
    return processor;
  }

  private static void logError(String message, Throwable error){
    CONSOLE.error(message, error);
  }

  private void preprocessIfNotSeenBefore(String workload, Iterable<DBIndex> hypotheticalIndexes) {
    if(!seenBefore(workload)){
      final CophyConstraints  defaultConstraints  = CophyConstraints.getDefaultConstraints();
      final GreedyResult      current             = result.get();
      try {
        result.compareAndSet(current,
            runCoPhy(workload, defaultConstraints, new ConsoleLogger(), hypotheticalIndexes));
      } catch (Exception e) {
        CONSOLE.error("Unable to run Cophy on " + workload, e);
      }
    }
  }
  
   // todo.Huascar...make this adaptable so that it could be used in dbtune.
   // todo..Huascar... this is the example that I need in order to implement INUM.
   // this is one of the first elements of the facade objects.
  private GreedyResult runCoPhy(String workloadName, CophyConstraints constr, LogListener listener,
      Iterable<DBIndex> hypotheticalIndexes) throws
      ParseException, IOException, IloException, SQLException {
    final WorkloadProcessor current = processor.get();
    processor.compareAndSet(current, initializeWorkloadProcessor(workloadName, hypotheticalIndexes));
    return new CophyAdapter(
        autopilot,
        processor.get()
    ).run(workloadName, constr, listener);
  }

  private static boolean seenBefore(String workload){
    return new File(InumUtils.getIndexAccessCostFile(workload)).exists();
  }


  /**
   * todo(Huascar) it needs to be injected in all its instances called by Cophy....
   */
  private static class CostEstimatorAdapter extends CostEstimator {
    CostEstimatorAdapter(DatabaseConnection connection, String workloadFile,
        Iterable<DBIndex> hypotheticalIndexes) throws IOException, ParseException {
      super(new Autopilot(connection), initializeWorkloadProcessor(workloadFile,
          hypotheticalIndexes), workloadFile);
    }

    //todo(Huascar) this is confussing. there are two Cplex classes: one that uses the CostEstimator
    // and another one that does not. The latter one uses a linear programming solver to determine
    // the queries costs. I will need to investigate whether I am missing a usecase that could
    // help me finalize the implementation of this what if optimizer.

  }

  /**
   * converts an INUM-Index to a dbtune DBIndex.
   */
  private static class IndexAdapter extends Index {

    IndexAdapter(DBIndex adaptee){
      this(adaptee.toString(), makeColumns(adaptee.getColumns()));
    }

    IndexAdapter(String tableName, Set<String> columns) {
      super(tableName, Objects.cast(columns, LinkedHashSet.class));
    }

    private static Set<String> makeColumns(Iterable<DatabaseColumn> cols){
      final Set<String> colsAsStrings = new LinkedHashSet<String>();
      for(DatabaseColumn each : cols){
         colsAsStrings.add(each.toString());
      }

      return colsAsStrings;
    }
  }

  /**
   * extends the workload processor by providing a method that takes dbtune's hypothetical
   * indexes and the converts them to inum;s indexes.
   */
  private static class WorkloadProcessorAdapter extends WorkloadProcessor {
    private final Iterable<DBIndex> hypotheticalIndexes;

    WorkloadProcessorAdapter(String fileName, Iterable<DBIndex> hypotheticalIndexes) throws ParseException {
      super(fileName);
      this.hypotheticalIndexes = hypotheticalIndexes;
    }

    void useHypotheticalIndexes(){
      if(Iterables.count(hypotheticalIndexes) == 0) return;
      final PhysicalConfiguration configuration = new PhysicalConfiguration();
      for(DBIndex each : hypotheticalIndexes){
        configuration.addIndex(new IndexAdapter(each));
      }

      this.universe = configuration;
      generateCandidatesFromConfig(configuration);

      final Set<String>       candidateSet = new HashSet<String>();
      final ArrayList<Index>  uniqList     = new ArrayList<Index>();
      for(Index each : candidates){
        final String key = each.getKey();
        if(!candidateSet.contains(key)) {
          uniqList.add(each);
          candidateSet.add(key);
        }
      }

      this.candidates = uniqList;
      CONSOLE.info("WorkloadProcessor is using " + candidates.size() + " candidates");
    }

    @Override protected void universeAndInterestingOrdersProcessing() {
      // do nothing
    }
  }

  /**
   * todo(Huascar) document
   */
  private static class CophyAdapter extends CoPhy {
    public CophyAdapter(autopilot autopilot, WorkloadProcessor processor) {
      super(autopilot, processor);
    }

    @Override protected GreedyResult processGeneratedIndexes(CPlex cplex, String workloadName)
        throws IOException, ParseException {
      return cplex.processGeneratedIndexes(workloadName, 
      /*todo(Huascar) may need a new instance of adapter rather than recycling one*/getWorkloadProcessor());
    }
  }

  /**
   * todo (Huascar): note to myself. The majority of methods calls in the autopilot
   * class finalized with a closing of the database connection. Since the responsibility
   * of closing the database is not this class's responsibility, I removed that logic from
   * all wrapped methods.
   */
  private static class Autopilot extends autopilot {
    private final Connection jdbcConnection;
    Autopilot(DatabaseConnection connection){
      super();
      this.jdbcConnection = connection.getJdbcConnection();
    }

    @Override public void disable_configuration(Configuration config) {
      if(config == null)                    { return; }
      if(config.implementedIndexes == null) { return; }

      final Connection conn = getConnection();
      final StopWatch  start  = new StopWatch();
      getAutopilotDelate().disable_configuration(config, conn);
      start.resetAndLog("disabling a configuration took");
    }

    @Override public void drop_configuration(PhysicalConfiguration configuration) {
      if(configuration == null) { return; }

      final Connection conn   = getConnection();
      final StopWatch  start  = new StopWatch();
      getAutopilotDelate().drop_configuration(configuration, conn);
      start.resetAndLog("dropping configuration took");
    }

    AutoPilotDelegate getAutopilotDelate(){
      final Object result = getFieldValue(this, "delegate");
      return (AutoPilotDelegate) Checks.checkNotNull(result);
    }

    @Override public Connection getConnection() {
      return jdbcConnection;
    }

    @Override public void init_database() {
      // do nothing since we already have a live java.sql.Connection object.
    }

    @Override public void dispose() {
      // do nothing since the autopilot does not need to terminate a db connection.
    }

    @Override public void implement_configuration(PhysicalConfiguration configuration) {
      if(configuration == null)                    { return; }
      final Connection conn = getConnection();
      final StopWatch  start  = new StopWatch();
      getAutopilotDelate().implement_configuration(this, configuration, conn);
      start.resetAndLog("physical configuration implementation took");
    }

    @Override public Plan optimizer_cost(String workload, boolean accessCost, boolean nlj) {
      Checks.checkArgument(!Strings.isEmpty(workload), "illegal workload.");
      final Connection conn = getConnection();
      try {
        return getAutopilotDelate().getExecutionPlan(conn, workload);
      } catch (SQLException e) {
        logError(String.format("unable to get the execution plan given workload: %s", workload), e);
        return null;
      }
    }

    private static Object getFieldValue(Object source, String name){
      try {
        Class<?> clazz = source.getClass();
        Field    field = clazz.getDeclaredField(name);
        while(field == null){
           try {
             field = clazz.getDeclaredField(name);
           } catch (NoSuchFieldException e){
             clazz = clazz.getSuperclass();
             if(clazz == null) return null;
           }
        }

        field.setAccessible(true);
        return field.get(source);
      } catch (Exception e){
        logError("unable to retrieve Field object from " + source, e);
        return null;
      }
    }
  }

}
