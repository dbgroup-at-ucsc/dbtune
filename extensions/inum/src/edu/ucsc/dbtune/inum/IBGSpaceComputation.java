package edu.ucsc.dbtune.inum;

import java.sql.SQLException;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.List;
import java.util.Set;
import java.util.Vector;

import edu.ucsc.dbtune.metadata.Catalog;
import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.metadata.Table;
import edu.ucsc.dbtune.optimizer.Optimizer;
import edu.ucsc.dbtune.optimizer.plan.InumPlan;
import edu.ucsc.dbtune.optimizer.plan.SQLStatementPlan;
import edu.ucsc.dbtune.optimizer.plan.TableAccessSlot;
import edu.ucsc.dbtune.workload.SQLStatement;

import static com.google.common.collect.Sets.cartesianProduct;

import static edu.ucsc.dbtune.optimizer.plan.Operator.NLJ;

/**
 * IBG-based INUM space computation.
 * 
 * @author Rui Wang
 * @author Ivo Jimenez
 */
public class IBGSpaceComputation implements InumSpaceComputation
{
    private boolean hasPlanWhichDontUseIndex;

    /**
     * TODO: complete.
     *
     * @param statement
     *      todo
     * @param delegate
     *      todo
     * @param indexes
     *      todo
     * @param inumSpace
     *      todo
     * @throws SQLException
     *      todo
     */
    public void ibg(
            SQLStatement statement,
            Optimizer delegate,
            HashSet<Index> indexes,
            Set<InumPlan> inumSpace)
        throws SQLException
    {
        SQLStatementPlan sqlPlan = delegate.explain(statement, indexes).getPlan();
        InumPlan templatePlan = new InumPlan(delegate, sqlPlan);

        Vector<Index> usedIndexes = new Vector<Index>();
        final Hashtable<Table, HashSet<Index>> tables = new Hashtable<Table, HashSet<Index>>();

        /**
         * TODO.
         */
        class A
        {
            /**
             * TODO.
             *
             * @param table
             *      TODO
             * @return
             *      TODO
             */
            HashSet<Index> getTable(Table table)
            {
                HashSet<Index> set = tables.get(table);
                if (set == null) {
                    set = new HashSet<Index>();
                    tables.put(table, set);
                }
                return set;
            }
        }

        A a = new A();
        // multiple interesting order are used in one table
        boolean conflict = false;
        for (TableAccessSlot slot : templatePlan.getSlots()) {
            Index usedIndex = slot.getIndex();
            if (usedIndex instanceof FullTableScanIndex)
                continue;
            usedIndexes.add(usedIndex);
            HashSet<Index> set = a.getTable(usedIndex.getTable());
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
        } else if (usedIndexes.size() == 0) {
            if (!hasPlanWhichDontUseIndex) {
                hasPlanWhichDontUseIndex = true;
                if (!sqlPlan.contains(NLJ))
                    inumSpace.add(templatePlan);
            }
        } else {
            if (!sqlPlan.contains(NLJ))
                inumSpace.add(templatePlan);
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
    public void compute(
            Set<InumPlan> space, SQLStatement statement, Optimizer delegate, Catalog catalog)
        throws SQLException
    {
        List<Set<Index>> indexesPerTable;
        DerbyInterestingOrdersExtractor interestingOrdersExtractor;

        interestingOrdersExtractor = new DerbyInterestingOrdersExtractor(catalog, true);
        indexesPerTable = interestingOrdersExtractor.extract(statement);
        HashSet<Index> allIndex = new HashSet<Index>();

        for (Set<Index> set : indexesPerTable)
            for (Index index : set)
                allIndex.add(index);

        space.clear();

        hasPlanWhichDontUseIndex = false;
        ibg(statement, delegate, allIndex, space);
    }
}
