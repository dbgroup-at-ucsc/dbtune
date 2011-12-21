package edu.ucsc.dbtune.inum;

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;

import java.sql.Connection;
import java.util.HashSet;
import java.util.Set;

import edu.ucsc.dbtune.metadata.Index;

import static java.lang.Double.compare;

/**
 * Default implementation of Inum's {@link MatchingStrategy}
 *
 * @author hsanchez@cs.ucsc.edu (Huascar A. Sanchez)
 */
public class InumMatchingStrategy implements MatchingStrategy
{
    private final IndexAccessCostEstimation accessCostEstimator;

    InumMatchingStrategy(IndexAccessCostEstimation accessCostEstimator)
    {
        this.accessCostEstimator = accessCostEstimator;
    }

    public InumMatchingStrategy(Connection connection)
    {
        this(new InumIndexAccessCostEstimation(Preconditions.checkNotNull(connection)));
    }

    @Override
    public double estimateCost(String query, Set<Index> inputConfiguration, InumSpace inumSpace)
    {
        // matched optimal plans for "query" ...
        final Set<OptimalPlan> matchedOptimalPlans = matches(
                query, inputConfiguration, inumSpace
                );
        // adding the cached cost + index access costs
        final double indexAccessCosts = getIndexAccessCostEstimation().estimateIndexAccessCost(query, inputConfiguration);
        final double derivedCost      = findOneWithMinCost(matchedOptimalPlans, indexAccessCosts);
        Preconditions.checkArgument(compare(derivedCost, 0.0) > 0, "invalid execution cost. It cannot be negative or zero.");
        return derivedCost;
    }

    private static double findOneWithMinCost(Set<OptimalPlan> matches, double indexAccessCost)
    {
        OptimalPlan min = null;
        double derivedCost = 0.0;
        for(OptimalPlan each : matches){
            if(null == min) { // base case
                min         = each;
                derivedCost = sumCachedCosts(each) + indexAccessCost;
                continue;
            }

            final double first  = sumCachedCosts(min)  + indexAccessCost;
            final double second = sumCachedCosts(each) + indexAccessCost;
            if(compare(second, first) < 0/*check if second is less than first*/) {
                min         = each;
                derivedCost = second;
            }
        }
        return derivedCost;
    }

    @Override
    public IndexAccessCostEstimation getIndexAccessCostEstimation()
    {
        return accessCostEstimator;
    }

    @Override
    public Set<OptimalPlan> matches(String sql, Set<Index> inputConfiguration, InumSpace inumSpace) 
    {
        final Set<OptimalPlan> found = Sets.newHashSet();
        // assuming there is a match, later methods will pick the optimal plan with the min cost.
        final Set<Index> copy = new HashSet<Index>(inputConfiguration);
        final Key targetKey = new Key(sql, copy);
        for(Key eachKey : inumSpace.keySet()){
            if(eachKey.equals/* means (same SQL and intersects indexes)*/(targetKey)) {
                final Set<OptimalPlan> optimalPlans = inumSpace.getOptimalPlans(eachKey);
                found.addAll(optimalPlans);
                break;
            }
        }
        return found;
    }

    private static double sumCachedCosts(OptimalPlan optimalPlan)
    {
        optimalPlan.computeInternalPlanCost();  // sum all subplans' costs.
        return optimalPlan.getInternalCost();
    }
}
