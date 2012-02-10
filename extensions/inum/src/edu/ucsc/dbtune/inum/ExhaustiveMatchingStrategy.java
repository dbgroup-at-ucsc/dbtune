package edu.ucsc.dbtune.inum;

import java.sql.SQLException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.optimizer.plan.InumPlan;

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
        InumPlan bestPlan = null;
        double bestCost = Double.POSITIVE_INFINITY;

        Set<List<Index>> atomicConfigurations = enumerateAtomicConfigurations(configuration);

        for (InumPlan templatePlan : inumSpace) {

            for (List<Index> atomicConfiguration : atomicConfigurations) {

                final double cost = templatePlan.plug(atomicConfiguration);

                if (cost <= bestCost) {
                    bestCost = cost;
                    bestConf = atomicConfiguration;
                    bestPlan = templatePlan;
                }
            }
        }
        
        if (bestPlan == null)
            throw new SQLException("Can't find match for configuration " + configuration);

        return new Result(bestPlan, new HashSet<Index>(bestConf), bestCost);
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
