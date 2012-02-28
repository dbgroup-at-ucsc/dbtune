package edu.ucsc.dbtune.inum;

import java.sql.SQLException;
import java.util.List;
import java.util.Set;

import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.optimizer.Optimizer;
import edu.ucsc.dbtune.optimizer.plan.InumPlan;
import edu.ucsc.dbtune.optimizer.plan.SQLStatementPlan;
import edu.ucsc.dbtune.workload.SQLStatement;

import static com.google.common.collect.Sets.cartesianProduct;
import static com.google.common.collect.Sets.newHashSet;

import static edu.ucsc.dbtune.optimizer.plan.Operator.NLJ;

/**
 * An implementation of the INUM space computation that populates it eagerly without any kind of 
 * optimization. This is in contrast to other more sophisticated strategies such as the ones 
 * outlined in [1], like <i>Lazy</i> and <i>Cost-based</i> evaluation.
 *
 * @author Ivo Jimenez
 * @author Quoc Trung Tran
 * @see <a href="http://portal.acm.org/citation.cfm?id=1325974"?>
 *          [1] Efficient use of the query optimizer for automated physical design
 *      </a>
 */
public class EagerSpaceComputation extends AbstractSpaceComputation
{
    /**
     * {@inheritDoc}
     */
    @Override
    public void computeWithCompleteConfiguration(
            Set<InumPlan> space,
            List<Set<Index>> indexesPerTable,
            SQLStatement statement,
            Optimizer delegate)
        throws SQLException
    {
        space.clear();

        for (List<Index> atomic : cartesianProduct(indexesPerTable)) {

            SQLStatementPlan sqlPlan = delegate.explain(statement, newHashSet(atomic)).getPlan();

            if (sqlPlan.contains(NLJ))
                continue;

            if (!isUsingAllInterestingOrders(sqlPlan, atomic))
                continue;

            space.add(new InumPlan(delegate, sqlPlan));
        }
    }
}
