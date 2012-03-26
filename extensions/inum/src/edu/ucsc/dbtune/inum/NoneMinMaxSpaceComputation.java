package edu.ucsc.dbtune.inum;

import java.sql.SQLException;

import java.util.Set;

import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.optimizer.Optimizer;
import edu.ucsc.dbtune.optimizer.plan.InumPlan;
import edu.ucsc.dbtune.workload.SQLStatement;

/**
 * none-min-max.
 *
 * @author Ivo Jimenez
 */
public class NoneMinMaxSpaceComputation extends AbstractSpaceComputation
{
    /**
     * {@inheritDoc}
     */
    @Override
    public void computeWithCompleteConfiguration(
            Set<InumPlan> space,
            Set<? extends Index> indexes,
            SQLStatement statement,
            Optimizer delegate)
        throws SQLException
    {
    }
}
