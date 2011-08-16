package edu.ucsc.dbtune.inum;

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import edu.ucsc.dbtune.core.DBIndex;
import edu.ucsc.dbtune.core.DatabaseConnection;
import edu.ucsc.dbtune.util.Strings;
import java.sql.Connection;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Default implementation of Inum's {@link Precomputation precomputation} step.
 *
 * @author hsanchez@cs.ucsc.edu (Huascar A. Sanchez)
 */
public class InumPrecomputation implements Precomputation {
  private final DatabaseConnection          connection;
  private final AtomicReference<InumSpace>  inumSpace;
  private final Set<String>                 seenWorkloads;


  public InumPrecomputation(DatabaseConnection connection){
    this.connection     = connection;
    this.inumSpace      = new AtomicReference<InumSpace>();
    this.seenWorkloads  = Sets.newHashSet();
  }


  @Override public InumSpace getInumSpace() {
    return Preconditions.checkNotNull(inumSpace.get());
  }

  @Override public Set<OptimalPlan> setup(String workload, Iterable<DBIndex> configuration) {
    if(inumSpace.get() == null) {
      inumSpace.set(new InmemoryInumSpace());
    }

    final Set<OptimalPlan> optimalPlans = Sets.newHashSet();

    // todo(Huascar)
    // call optimizer given the workload and an input configuration
    //   get optimal plan as a String
    //   parse it and collect information needed to create a new instance of Optimal plan
    //   add all returned plans to optimalPlans
    //   save plans in InumSpace
    // return a reference to the set of optimal plans
    final String queryExecutionPlan = getQueryExecutionPlan(connection.getJdbcConnection(),
        workload, configuration);
    if(Strings.isEmpty(queryExecutionPlan)) return optimalPlans;

    optimalPlans.addAll(buildPlans(queryExecutionPlan));

    return getInumSpace().save(optimalPlans);
  }

  private static String getQueryExecutionPlan(Connection connection, String query,
      Iterable<DBIndex> configuration){
    return "";
  }

  // parsing plan suggested by optimizer
  private static Set<OptimalPlan> buildPlans(String queryExecutionPlan){
    // todo(Huascar) implement this.
    return Sets.newHashSet();
  }



  @Override public boolean skip(String workload) {
    return seenWorkloads.contains(workload);
  }
}
