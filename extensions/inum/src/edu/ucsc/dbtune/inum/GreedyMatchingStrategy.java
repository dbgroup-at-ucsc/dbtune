package edu.ucsc.dbtune.inum;

import java.sql.SQLException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.metadata.Table;
import edu.ucsc.dbtune.optimizer.plan.InumPlan;
import edu.ucsc.dbtune.optimizer.plan.SQLStatementPlan;

import static edu.ucsc.dbtune.util.MetadataUtils.getIndexesPerTable;

/**
 * Exhaustive matching strategy.
 *
 * @author Ivo Jimenez
 */
public class GreedyMatchingStrategy extends AbstractMatchingStrategy
{
    /**
     * {@inheritDoc}
     */
    @Override
    public Result matchCompleteConfiguration(Set<InumPlan> inumSpace, Set<Index> configuration)
        throws SQLException
    {
        if (inumSpace.size() == 0)
            throw new SQLException("No template plan in the INUM space");
        
        Set<Index> bestConf = null;
        InumPlan bestTemplate = null;
        SQLStatementPlan instantiatedPlan = null;
        Index  bestIndexForSlot = null;
        double bestCostForIndex = Double.POSITIVE_INFINITY;
        double bestCost = Double.POSITIVE_INFINITY;

        Map<Table, Set<Index>> indexesPerTable = getIndexesPerTable(configuration);

        // main idea: iterate over each template. For each template iterate
        // over each slot and find the best index from the given
        // configuration that can be plugged into it. The best template
        // instantiation is the winner.

        for (InumPlan template : inumSpace) {

            Set<Index> bestAtomicConfiguration = new HashSet<Index>();

            for (Table table : template.getTables()) {

                Set<Index> indexesForTable = indexesPerTable.get(table);

                if (indexesForTable == null)
                    throw new SQLException("No indexes for " + table);

                bestIndexForSlot = null;
                bestCostForIndex = Double.POSITIVE_INFINITY;

                for (Index i : indexesForTable) {

                    double costForIndex = template.plug(i);

                    if (costForIndex < bestCostForIndex) {
                        bestCostForIndex = costForIndex;
                        bestIndexForSlot = i;
                    }
                }

                if (bestIndexForSlot == null)
                    // didn't find a matching index for this template
                    // need to check the next template
                    break;

                bestAtomicConfiguration.add(bestIndexForSlot);
            }

            if (bestAtomicConfiguration.size() != template.getTables().size())
                // not all indexes where compatible, check the next template
                continue;

            SQLStatementPlan plan = template.instantiate(bestAtomicConfiguration);

            if (plan == null)
                throw new SQLException("Can't instantiate best configuration; check compatibility");

            if (plan.getRootOperator().getAccumulatedCost() < bestCost) {
                bestCost = plan.getRootOperator().getAccumulatedCost();
                bestConf = bestAtomicConfiguration;
                bestTemplate = template;
                instantiatedPlan = plan;
            }
        }
        
        if (bestTemplate == null)
            throw new SQLException("Can't find match for configuration " + configuration);

        return new Result(instantiatedPlan, bestTemplate, new HashSet<Index>(bestConf), bestCost);
    }
}
