package edu.ucsc.dbtune.inum;

import java.sql.SQLException;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.google.common.collect.Sets;

import edu.ucsc.dbtune.metadata.Catalog;
import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.optimizer.Optimizer;
import edu.ucsc.dbtune.optimizer.plan.InumPlan;
import edu.ucsc.dbtune.util.BitArraySet;
import edu.ucsc.dbtune.workload.SQLStatement;

/**
 * @author Ivo Jimenez
 */
public class EagerSpaceComputation implements InumSpaceComputation
{
    /**
     * {@inheritDoc}
     */
    public Set<InumPlan> compute(SQLStatement stmt, Optimizer delegate, Catalog catalog)
        throws SQLException
    {
        Set<InumPlan> plans = new HashSet<InumPlan>();
        DerbyInterestingOrdersExtractor ioE = new DerbyInterestingOrdersExtractor(catalog, true);
        List<Set<Index>> sets = ioE.extract(stmt);

        for (List<Index> o : Sets.cartesianProduct(sets))
            plans.add(new InumPlan(delegate.explain(stmt, new BitArraySet<Index>(o)).getPlan()));

        return plans;
    }
}
