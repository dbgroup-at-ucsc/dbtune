package edu.ucsc.dbtune.core;

import edu.cmu.db.autopilot.AutoPilotDelegate;
import edu.cmu.db.autopilot.autopilot;
import edu.cmu.db.inum.PostgresIndexAccessGenerator;
import edu.cmu.db.model.Configuration;
import edu.cmu.db.model.Index;
import edu.cmu.db.model.PhysicalConfiguration;
import edu.cmu.db.model.Plan;
import edu.cmu.db.model.QueryDesc;
import edu.ucsc.dbtune.spi.Environment;
import edu.ucsc.dbtune.spi.core.Console;
import edu.ucsc.dbtune.util.Checks;
import edu.ucsc.dbtune.util.StopWatch;
import edu.ucsc.dbtune.util.Strings;
import edu.ucsc.satuning.util.Objects;
import java.io.File;
import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Set;

/**
 * default implementation of {@link InumWhatIfOptimizer} type.
 *
 * @author hsanchez@cs.ucsc.edu (Huascar A. Sanchez)
 */
// todo(Huascar) next? work on the caching of workloads logic
class InumWhatIfOptimizerImpl implements InumWhatIfOptimizer {
  private final Set<String>         queries;
  private final Autopilot           autopilot;

  private static final Console CONSOLE = Console.streaming();
  private static final Set<String> WORKLOADS = new HashSet<String>();
  static {
    final Environment                 env       = Environment.getInstance();
    final WorkloadDirectoryNode directory       = new WorkloadDirectoryNode(
        new File(env.getOutputFoldername())
    );
    final WalkThroughWorkloadsVisitor visitor   = new WalkThroughWorkloadsVisitor();
    WORKLOADS.addAll(directory.accept(visitor));
  }

  /**
   * construct a new {@code InumWhatIfOptimizer} object.
   * @param connection
   */
  InumWhatIfOptimizerImpl(DatabaseConnection connection){
    this(new HashSet<String>(), new Autopilot(checkDatabaseConnection(connection)));
  }

  /**
   * construct a new {@code InumWhatIfOptimizer} object given a set of queries (workload) and
   * an {@link autopilot autopilot object} for {@code INUM}.
   * @param queries
   *    a workload that could be either empty or non-empty.
   * @param autopilot
   *    an {@link autopilot autopilot object} required by {@code INUM} to deal with their
   *    physical configurations.
   */
  InumWhatIfOptimizerImpl(Set<String> queries, autopilot autopilot){
    this.queries    = Checks.checkNotNull(queries);
    this.autopilot  = (Autopilot) autopilot;
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
    preprocessIfSeenBefore(workload);
    final Iterable<DBIndex> indexes = Checks.checkNotNull(hypotheticalIndexes);
    double cost;

    final String    query     = checkAndAdd(workload);
    final QueryDesc queryDesc = new QueryDesc();
    queryDesc.queryString     = query;

    // create a physical confirmation
    final PhysicalConfiguration pc = new PhysicalConfiguration();
    for(DBIndex each : indexes){
      pc.addIndex(new IndexAdapter(each));
    }

    PostgresIndexAccessGenerator generator;
    try {
      generator = new PostgresIndexAccessGenerator(autopilot);
    } catch (Exception ignored){
      throw new IndexAccessGenerationException("unable to create an index access generator");
    }

    final String plan = generator.indexAccessCost(pc, queryDesc, true);
    cost = getPlanCost(plan);
    return cost;
  }

  private void preprocessIfSeenBefore(String workload) {
    if(!seenBefore(workload)){
       // perform
    }
  }

  public static double getPlanCost(String plan){
		/*get cost from plan --> cost=val1..val2 */
    final int pos1 = plan.indexOf("..");
    final int pos2 = plan.indexOf(" ", pos1);
    final String costTotal = plan.substring(pos1 + 2, pos2);
		return new Double(costTotal);
  }

  private String checkAndAdd(String workload){
    if(queries.contains(workload)) return workload;
    queries.add(workload);
    return workload;
  }

  /**
   *
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

  private static void logError(String message, Throwable error){
    CONSOLE.error(message, error);
  }

  private static boolean seenBefore(String workload){
    return WORKLOADS.contains(workload);
  }

}
