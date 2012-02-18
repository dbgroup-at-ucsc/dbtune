package edu.ucsc.dbtune.inum;

import java.sql.SQLException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.optimizer.plan.InumPlan;
import edu.ucsc.dbtune.optimizer.plan.SQLStatementPlan;

import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Sets.cartesianProduct;

import static edu.ucsc.dbtune.util.MetadataUtils.getIndexesPerTable;

/**
 * Exhaustive matching strategy.
 *
 * @author Ivo Jimenez
 */
public class ExhaustiveMatchingStrategy extends AbstractMatchingStrategy
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
        
        List<Index> bestConf = null;
        InumPlan bestTemplate = null;
        SQLStatementPlan instantiatedPlan = null;
        double bestCost = Double.POSITIVE_INFINITY;
        double cost;

        Set<List<Index>> atomicConfigurations = enumerateAtomicConfigurations(configuration);

        for (InumPlan template : inumSpace) {

            for (List<Index> atomicConfiguration : atomicConfigurations) {

                SQLStatementPlan plan = template.instantiate(atomicConfiguration);

                if (plan == null)
                    continue;

                cost = plan.getRootOperator().getAccumulatedCost();

                if (cost < bestCost) {
                    bestCost = cost;
                    bestConf = atomicConfiguration;
                    bestTemplate = template;
                    instantiatedPlan = plan;
                }
            }
        }
        
        if (bestTemplate == null)
            throw new SQLException("Can't find match for configuration " + configuration);

        return new Result(instantiatedPlan, bestTemplate, new HashSet<Index>(bestConf), bestCost);
    }

    /**
     * @param indexes
     *      indexes from which the atomic configurations are enumerated
     * @return
     *      all the possible atomic configurations
     */
    private static Set<List<Index>> enumerateAtomicConfigurations(Set<Index> indexes)
    {
        return cartesianProduct(newArrayList(getIndexesPerTable(indexes).values()));
    }
}
