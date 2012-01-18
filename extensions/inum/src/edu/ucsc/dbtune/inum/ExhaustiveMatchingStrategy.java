package edu.ucsc.dbtune.inum;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.Sets;

import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.metadata.Table;
import edu.ucsc.dbtune.optimizer.plan.InumPlan;
import edu.ucsc.dbtune.util.BitArraySet;

/**
 * @author Ivo Jimenez
 */
public class ExhaustiveMatchingStrategy implements MatchingStrategy
{
    /**
     */
    public ExhaustiveMatchingStrategy()
    {
    }

    /**
     * @param templatePlans
     *      plans that are iterated in order to look for the best
     * @param configuration
     *      the configuration considered to estimate the cost of the new statement. This can (or 
     *      not) be the same as {@link #getConfiguration}.
     * @return
     *      a new statement
     * @throws SQLException
     *      if it's not possible to do what-if optimization on the given configuration
     */
    public Result match(Set<InumPlan> templatePlans, Set<Index> configuration)
        throws SQLException
    {
        List<Index> bestConf = null;
        InumPlan bestPlan = null;
        double bestCost = Double.MAX_VALUE;

        Set<List<Index>> atomicConfigurations = enumerateAtomicConfigurations(configuration);

        for (InumPlan plan : templatePlans)
            for (List<Index> atomicConfiguration : atomicConfigurations) {
                final double cost = plan.plugIntoSlots(new BitArraySet<Index>(atomicConfiguration));

                if (cost < bestCost) {
                    bestCost = cost;
                    bestConf = atomicConfiguration;
                    bestPlan = plan;
                }
            }
        
        if (bestPlan == null)
            throw new SQLException("Can't find match for configuration " + configuration);

        return new Result(bestPlan, new HashSet<Index>(bestConf), bestCost);
    }

    /**
     */
    public static Set<List<Index>> enumerateAtomicConfigurations(Set<Index> indexes)
    {
        Map<Table, Set<Index>> interestingOrdersPerTable = new HashMap<Table, Set<Index>>();
        Set<Index> interestingOrdersForTable;

        for (Index i : indexes) {

            interestingOrdersForTable = interestingOrdersPerTable.get(i.getTable());

            if (interestingOrdersForTable == null) {
                interestingOrdersForTable = new BitArraySet<Index>();
                interestingOrdersPerTable.put(i.getTable(), interestingOrdersForTable);
            }

            interestingOrdersForTable.add(i);
        }

        return Sets.cartesianProduct(new ArrayList<Set<Index>>(interestingOrdersPerTable.values()));
    }
}
