package edu.ucsc.dbtune.inum;

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.spi.Console;
import edu.ucsc.dbtune.util.Combinations;
import edu.ucsc.dbtune.util.Strings;
import java.sql.Connection;
import java.util.Set;

/**
 * Default implementation of Inum's {@link Precomputation precomputation} step.
 *
 * @author hsanchez@cs.ucsc.edu (Huascar A. Sanchez)
 */
public class InumPrecomputation implements Precomputation {
  private final OptimalPlanProvider         provider;
  private final OptimalPlansParser          parser;
  private final InumSpace                   inumSpace;
  private final Set<String>                 seenWorkloads;

  InumPrecomputation(InumSpace inumSpace,
      OptimalPlanProvider provider, OptimalPlansParser parser){
    this.provider         = provider;
    this.parser           = parser;
    this.inumSpace        = inumSpace;
    this.seenWorkloads    = Sets.newHashSet();
  }

  public InumPrecomputation(Connection connection){
    this(new InMemoryInumSpace(), new SqlExecutionPlanProvider(connection), new InumOptimalPlansParser());
  }

  private void addQuerytoListOfSeenQueries(String query){
    Preconditions.checkArgument(!Strings.isEmpty(query));
    seenWorkloads.add(query);
  }

  @Override public InumSpace getInumSpace() {
    return inumSpace;
  }

  @Override public InumSpace setup(String query, Set<Index> interestingOrders) {
    addQuerytoListOfSeenQueries(query);
    // generate all possible interesting orders combinations (atomic) that will be used
    // during the INUM's {@link Precomputation setup} phase.
    final Set<Set<Index>> allAtomicCombOfInterestingOrders = 
        Combinations.setOfAllSubsets(interestingOrders);
    for(Set<Index> o /*o as in the JavaDoc*/
        : allAtomicCombOfInterestingOrders){
      final Set<OptimalPlan> optimalPlansPerInterestingOrder = Sets.newHashSet();
      // call optimizer given the workload and an input configuration
      //   get optimal plan as a String
      //   parse it and collect information needed to create a new instance of Optimal plan
      //   add all returned plans to optimalPlans
      //   save plans in InumSpace indexed by interesting order.
      // return a reference to the set of optimal plans
      final String queryExecutionPlan = provider.getSqlExecutionPlan(query, o);
      if(Strings.isEmpty(queryExecutionPlan)) continue;
      optimalPlansPerInterestingOrder.add(parser.parse(queryExecutionPlan));

      final QueryRecord key = new QueryRecord(query, o);
      getInumSpace().save(key, optimalPlansPerInterestingOrder);

      Console.streaming().info(
          String.format("%d optimal plans were cached for %s key.",
              getInumSpace().getOptimalPlans(key).size(),
              key
          )
      );

    }

    return getInumSpace();
  }


  @Override public boolean skip(String query) {
    return seenWorkloads.contains(query);
  }
}
