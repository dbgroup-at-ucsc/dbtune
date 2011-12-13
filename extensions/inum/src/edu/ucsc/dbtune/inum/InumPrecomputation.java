package edu.ucsc.dbtune.inum;

import edu.ucsc.dbtune.metadata.Configuration;
import edu.ucsc.dbtune.spi.Console;
import edu.ucsc.dbtune.util.Combinations;
import edu.ucsc.dbtune.util.Strings;

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;

import java.sql.Connection;
import java.util.Set;

/**
 * Default implementation of Inum's {@link Precomputation precomputation} step.
 *
 * @author hsanchez@cs.ucsc.edu (Huascar A. Sanchez)
 */
public class InumPrecomputation implements Precomputation
{
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

  public InumPrecomputation(Connection connection)
  {
    this(new InMemoryInumSpace(), new SqlExecutionPlanProvider(connection), new InumOptimalPlansParser());
  }

  private void addQuerytoListOfSeenQueries(String query)
  {
    Preconditions.checkArgument(!Strings.isEmpty(query));
    if (!seenWorkloads.contains(query)){
      seenWorkloads.add(query);
    }
  }

  @Override public InumSpace getInumSpace()
 {
    return inumSpace;
  }

  @Override public Set<OptimalPlan> setup(String query, Configuration interestingOrders)
 {
    addQuerytoListOfSeenQueries(query);
    // generate all possible interesting orders combinations (atomic) that will be used
    // during the INUM's {@link Precomputation setup} phase.
    final Set<Configuration> allAtomicCombOfInterestingOrders = Combinations.findCrossProduct(interestingOrders);
    for (Configuration o /*o as in the JavaDoc*/
        : allAtomicCombOfInterestingOrders){
      final Set<OptimalPlan> optimalPlansPerInterestingOrder = Sets.newHashSet();
      // call optimizer given the workload and an input configuration
      //   get optimal plan as a String
      //   parse it and collect information needed to create a new instance of Optimal plan
      //   add all returned plans to optimalPlans
      //   save plans in InumSpace indexed by interesting order.
      // return a reference to the set of optimal plans
      final String queryExecutionPlan = provider.getSqlExecutionPlan(query, o);
      if (Strings.isEmpty(queryExecutionPlan)) continue;
      optimalPlansPerInterestingOrder.addAll(parser.parse(queryExecutionPlan));

      final Set<OptimalPlan> referenceToPlans = getInumSpace().save(
          o,
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


  @Override public boolean skip(String query)
 {
    return seenWorkloads.contains(query);
  }
}
