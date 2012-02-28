package edu.ucsc.dbtune.inum;

import java.sql.SQLException;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.List;
import java.util.Set;
import java.util.Vector;

import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.metadata.Table;
import edu.ucsc.dbtune.optimizer.Optimizer;
import edu.ucsc.dbtune.optimizer.plan.InumPlan;
import edu.ucsc.dbtune.optimizer.plan.SQLStatementPlan;
import edu.ucsc.dbtune.workload.SQLStatement;

import static com.google.common.collect.Sets.cartesianProduct;

import static edu.ucsc.dbtune.optimizer.plan.Operator.NLJ;

/**
 * IBG-based INUM space computation.
 * 
 * @author Rui Wang
 * @author Ivo Jimenez
 */
public class IBGSpaceComputation extends AbstractSpaceComputation
{
    private boolean hasPlanWhichDontUseIndex;
    private InumPlan templateForEmpty;

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
            HashSet<Index> indexes,
            Set<InumPlan> inumSpace)
        throws SQLException
    {
        SQLStatementPlan sqlPlan = delegate.explain(statement, indexes).getPlan();

        Vector<Index> usedIndexes = new Vector<Index>();
        final Hashtable<Table, HashSet<Index>> tables = new Hashtable<Table, HashSet<Index>>();

        // multiple interesting order are used in one table
        boolean conflict = false;
        for (Index usedIndex : sqlPlan.getIndexes()) {
            if (usedIndex instanceof FullTableScanIndex)
                continue;
            usedIndexes.add(usedIndex);

            HashSet<Index> set = tables.get(usedIndex.getTable());
            if (set == null) {
                set = new HashSet<Index>();
                tables.put(usedIndex.getTable(), set);
            }
            set.add(usedIndex);
            if (set.size() > 1)
                conflict = true;
        }

        if (conflict) {
            Vector<HashSet<Index>> conflictInterestingOrder = new Vector<HashSet<Index>>();
            Vector<Index> validInterestingOrder = new Vector<Index>();
            for (HashSet<Index> set : tables.values()) {
                if (set.size() > 1)
                    conflictInterestingOrder.add(set);
                else
                    validInterestingOrder.add((Index) set.toArray()[0]);
            }
            for (List<Index> atomic : cartesianProduct(conflictInterestingOrder)) {
                HashSet<Index> set = new HashSet<Index>();
                set.addAll(validInterestingOrder);
                for (Index index : atomic)
                    set.add(index);
                ibg(statement, delegate, set, inumSpace);
            }
        } else if (usedIndexes.isEmpty()) {
            if (!hasPlanWhichDontUseIndex) {
                hasPlanWhichDontUseIndex = true;
                templateForEmpty = new InumPlan(delegate, sqlPlan);
                //if (!sqlPlan.contains(NLJ))
                inumSpace.add(templateForEmpty);
            }
        } else {
            if (!sqlPlan.contains(NLJ))
                inumSpace.add(new InumPlan(delegate, sqlPlan));
            for (Index usedIndex : usedIndexes) {
                HashSet<Index> set = new HashSet<Index>();
                set.addAll(indexes);
                set.remove(usedIndex);
                ibg(statement, delegate, set, inumSpace);
            }
        }
    }

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
        HashSet<Index> allIndex = new HashSet<Index>();

        space.clear();

        for (Set<Index> set : indexesPerTable)
            for (Index index : set)
                allIndex.add(index);

        hasPlanWhichDontUseIndex = false;
        ibg(statement, delegate, allIndex, space);
    }
}
