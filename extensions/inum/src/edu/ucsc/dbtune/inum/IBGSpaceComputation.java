package edu.ucsc.dbtune.inum;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.optimizer.Optimizer;
import edu.ucsc.dbtune.optimizer.plan.InumPlan;
import edu.ucsc.dbtune.optimizer.plan.SQLStatementPlan;
import edu.ucsc.dbtune.workload.SQLStatement;

import static com.google.common.collect.Sets.cartesianProduct;

import static edu.ucsc.dbtune.optimizer.plan.Operator.NLJ;
import static edu.ucsc.dbtune.util.MetadataUtils.getIndexesPerTable;

/**
 * IBG-based INUM space computation.
 * 
 * @author Rui Wang
 * @author Ivo Jimenez
 */
public class IBGSpaceComputation extends AbstractSpaceComputation
{
    /**
     * TODO: complete.
     * 
     * @param statement
     *            todo
     * @param delegate
     *            todo
     * @param indexes
     *            todo
     * @param inumSpace
     *            todo
     * @throws SQLException
     *             todo
     */
    public void ibg(
            SQLStatement statement,
            Optimizer delegate,
            Set<Index> indexes,
            Set<InumPlan> inumSpace)
        throws SQLException
    {
        if (indexes.isEmpty())
            return;

        SQLStatementPlan sqlPlan = delegate.explain(statement, indexes).getPlan();

        List<Set<Index>> intersectedIndexes = new ArrayList<Set<Index>>();
        Set<Index> notIntersectedIndexes = new HashSet<Index>();

        for (Set<Index> indexesForTable : getIndexesPerTable(sqlPlan.getIndexes()).values()) {
            if (indexesForTable.size() > 1)
                intersectedIndexes.add(indexesForTable);
            else
                notIntersectedIndexes.addAll(indexesForTable);
        }

        if (!intersectedIndexes.isEmpty()) {

            for (List<Index> atomic : cartesianProduct(intersectedIndexes)) {
                Set<Index> conf = new HashSet<Index>();

                conf.addAll(notIntersectedIndexes);
                conf.addAll(atomic);

                ibg(statement, delegate, conf, inumSpace);
            }
        } else {
            if (!sqlPlan.contains(NLJ) &&
                    isUsingAllInterestingOrders(sqlPlan, sqlPlan.getIndexes()))
                inumSpace.add(new InumPlan(delegate, sqlPlan));

            for (Index usedIndex : sqlPlan.getIndexes()) {
                Set<Index> conf = new HashSet<Index>();

                conf.addAll(indexes);
                conf.remove(usedIndex);

                ibg(statement, delegate, conf, inumSpace);
            }
        }
    }

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
        HashSet<Index> allIndexes = new HashSet<Index>();

        space.clear();

        // add FTS-everywhere
        space.add(
            new InumPlan(
                delegate, delegate.explain(statement, new HashSet<Index>()).getPlan()));

        for (Index i : indexes)
            allIndexes.add(i);

        ibg(statement, delegate, allIndexes, space);
    }
}
