package edu.ucsc.dbtune.inum;

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import edu.ucsc.dbtune.core.DBIndex;
import edu.ucsc.dbtune.core.DatabaseConnection;
import edu.ucsc.dbtune.spi.core.Console;
import edu.ucsc.dbtune.util.Combinations;
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
  private final OptimalPlansParser          parser;
  private final AtomicReference<InumSpace>  inumSpace;
  private final Set<String>                 seenWorkloads;

  InumPrecomputation(DatabaseConnection connection, OptimalPlansParser parser){
    this.connection     = connection;
    this.parser         = parser;
    this.inumSpace      = new AtomicReference<InumSpace>();
    this.seenWorkloads  = Sets.newHashSet();
  }
  public InumPrecomputation(DatabaseConnection connection){
    this(connection, new InumOptimalPlansParser());
  }

  private void addQuerytoListOfSeenQueries(String query){
    Preconditions.checkArgument(!Strings.isEmpty(query));
    if(!seenWorkloads.contains(query)){
      seenWorkloads.add(query);
    }
  }

  @Override public InumSpace getInumSpace() {
    return Preconditions.checkNotNull(inumSpace.get());
  }

  @Override public Set<OptimalPlan> setup(String query, Iterable<DBIndex> interestingOrders) {
    if(inumSpace.get() == null) {
      inumSpace.set(new InMemoryInumSpace());
    }

    addQuerytoListOfSeenQueries(query);
    final Set<Set<DBIndex>> allCombinationsOfInterestingOrders = Combinations.findCombinations(interestingOrders);
    for(Set<DBIndex> eachIOs : allCombinationsOfInterestingOrders){
      final Set<OptimalPlan> optimalPlansPerInterestingOrder = Sets.newHashSet();
      // call optimizer given the workload and an input configuration
      //   get optimal plan as a String
      //   parse it and collect information needed to create a new instance of Optimal plan
      //   add all returned plans to optimalPlans
      //   save plans in InumSpace indexed by interesting order.
      // return a reference to the set of optimal plans
      final String queryExecutionPlan = getQueryExecutionPlan(   // get execution plan given the set of interesting orders.
          connection.getJdbcConnection(),
          query,
          eachIOs
      );
      if(Strings.isEmpty(queryExecutionPlan)) continue;
      optimalPlansPerInterestingOrder.addAll(parser.parse(queryExecutionPlan));

      final Set<OptimalPlan> referenceToPlans = getInumSpace().save(
          eachIOs,
          optimalPlansPerInterestingOrder
      );

      Console.streaming().info(
          String.format("%d optimal plans were cached for %s interesting order.",
              referenceToPlans.size(),
              interestingOrders
          )
      );

    }

    return getInumSpace().getAllSavedOptimalPlans();
  }

  //todo(Huascar) to implement once the dbms changes are done
  private static String getQueryExecutionPlan(Connection connection, String query,
      Iterable<DBIndex> interestingOrders){
    // example of a possible suggested plan
    return "Hash Join  (cost=174080.39..9364262539.50 rows=1 width=193)";   // we can have one or many query plans
  }


  @Override public boolean skip(String query) {
    return seenWorkloads.contains(query);
  }
}
