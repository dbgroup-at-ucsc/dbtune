package edu.ucsc.dbtune.inum;

import java.sql.SQLException;

import java.util.HashSet;
import java.util.Set;

import edu.ucsc.dbtune.metadata.Catalog;
import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.optimizer.Optimizer;
import edu.ucsc.dbtune.optimizer.plan.InumPlan;
import edu.ucsc.dbtune.workload.SQLStatement;

import static edu.ucsc.dbtune.util.InumUtils.getMaximumAtomicConfiguration;
import static edu.ucsc.dbtune.util.InumUtils.getMinimumAtomicConfiguration;

/**
 * none-min-max.
 *
 * @author Ivo Jimenez
 */
public class NoneMinMaxSpaceComputation implements InumSpaceComputation
{
    private static Set<Index> empty = new HashSet<Index>();

    /**
     * {@inheritDoc}
     */
    @Override
    public void compute(
            Set<InumPlan> space, SQLStatement statement, Optimizer delegate, Catalog catalog)
        throws SQLException
    {
        InumPlan templateForEmpty =
            new InumPlan(delegate, delegate.explain(statement, empty).getPlan());

        space.add(templateForEmpty);

        Set<Index> min = getMinimumAtomicConfiguration(templateForEmpty);
        Set<Index> max = getMaximumAtomicConfiguration(statement, catalog);
        
        space.add(new InumPlan(delegate, delegate.explain(statement, min).getPlan()));
        space.add(new InumPlan(delegate, delegate.explain(statement, max).getPlan()));
    }
}
