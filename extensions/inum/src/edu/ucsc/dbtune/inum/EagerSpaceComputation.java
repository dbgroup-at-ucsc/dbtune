package edu.ucsc.dbtune.inum;

import java.sql.SQLException;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import edu.ucsc.dbtune.metadata.Catalog;
import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.metadata.Table;
import edu.ucsc.dbtune.optimizer.Optimizer;
import edu.ucsc.dbtune.optimizer.plan.InumPlan;
import edu.ucsc.dbtune.workload.SQLStatement;

import static com.google.common.collect.Sets.cartesianProduct;
import static edu.ucsc.dbtune.util.MetadataUtils.getTables;

/**
 * An implementation of the INUM space computation that populates it eagerly without any kind of 
 * optimization. This is in contrast to other more sophisticated strategies such as the ones 
 * outlined in [1], like <i>Lazy</i> and <i>Cost-based</i> evaluation.
 * <p>
 * This implementation assumes that the plans obtained by the delegate don't contain NLJ operators.
 *
 * @author Ivo Jimenez
 * @see <a href="http://portal.acm.org/citation.cfm?id=1325974"?>
 *          [1] Efficient use of the query optimizer for automated physical design
 *      </a>
 */
public class EagerSpaceComputation implements InumSpaceComputation
{
    /**
     * {@inheritDoc}
     */
    public Set<InumPlan> compute(SQLStatement statement, Optimizer delegate, Catalog catalog)
        throws SQLException
    {
        Set<InumPlan> plans;
        DerbyInterestingOrdersExtractor ioE;
        List<Set<Index>> indexesPerTable;

        ioE = new DerbyInterestingOrdersExtractor(catalog, true);
        indexesPerTable = ioE.extract(statement);

        plans = new HashSet<InumPlan>();

        for (List<Index> atomic : cartesianProduct(indexesPerTable)) {

            delegate.setFTSDisabled(getTables(atomic), true);

            plans.add(
                    new InumPlan(
                        delegate.explain(statement, new HashSet<Index>(atomic)).getPlan()));

            delegate.setFTSDisabled(new HashSet<Table>(), false);
        }

        return plans;
    }
}
