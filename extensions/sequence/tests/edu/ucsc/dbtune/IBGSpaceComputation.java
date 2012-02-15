package edu.ucsc.dbtune;

import static com.google.common.collect.Sets.cartesianProduct;
import static edu.ucsc.dbtune.optimizer.plan.Operator.NLJ;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.Vector;

import edu.ucsc.dbtune.inum.DerbyInterestingOrdersExtractor;
import edu.ucsc.dbtune.inum.FullTableScanIndex;
import edu.ucsc.dbtune.inum.InumInterestingOrder;
import edu.ucsc.dbtune.inum.InumSpaceComputation;
import edu.ucsc.dbtune.metadata.Catalog;
import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.optimizer.Optimizer;
import edu.ucsc.dbtune.optimizer.plan.InumPlan;
import edu.ucsc.dbtune.optimizer.plan.SQLStatementPlan;
import edu.ucsc.dbtune.optimizer.plan.TableAccessSlot;
import edu.ucsc.dbtune.seq.utils.RTimer;
import edu.ucsc.dbtune.seq.utils.Rt;
import edu.ucsc.dbtune.workload.SQLStatement;

/**
 * Copy from EagerSpaceComputation
 * 
 * @author Ivo Jimenez
 * @author Quoc Trung Tran
 * @author Rui Wang
 */
public class IBGSpaceComputation implements InumSpaceComputation {
    boolean hasPlanWhichDontUseIndex = false;
    boolean verbose = true;

    public void ibg(SQLStatement statement, Optimizer delegate,
            HashSet<Index> indexes, Set<InumPlan> inumSpace)
            throws SQLException {
        SQLStatementPlan sqlPlan = delegate.explain(statement, indexes)
                .getPlan();
        InumPlan templatePlan = new InumPlan(delegate, sqlPlan);

        Vector<Index> usedIndexes = new Vector<Index>();
        for (TableAccessSlot slot : templatePlan.getSlots()) {
            Index usedIndex = slot.getIndex();
            if (usedIndex instanceof FullTableScanIndex)
                continue;
            usedIndexes.add(usedIndex);
        }
        if (usedIndexes.size() == 0) {
            if (!hasPlanWhichDontUseIndex) {
                hasPlanWhichDontUseIndex = true;
                inumSpace.add(templatePlan);
            }
        } else {
            inumSpace.add(templatePlan);
            for (Index usedIndex : usedIndexes) {
                HashSet<Index> set = new HashSet<Index>();
                set.addAll(indexes);
                set.remove(usedIndex);
                ibg(statement, delegate, set, inumSpace);
            }
        }
    }

    @Override
    public Set<InumPlan> compute(SQLStatement statement, Optimizer delegate,
            Catalog catalog) throws SQLException {
        List<Set<Index>> indexesPerTable;
        Set<InumPlan> inumSpace;
        List<Index> minimumAtomic;
        DerbyInterestingOrdersExtractor interestingOrdersExtractor;
        SQLStatementPlan sqlPlan;
        InumPlan templatePlan;

        interestingOrdersExtractor = new DerbyInterestingOrdersExtractor(
                catalog, true);
        indexesPerTable = interestingOrdersExtractor.extract(statement);
        HashSet<Index> allIndex = new HashSet<Index>();
        Rt.showDate=false;
        Rt.p("building INUM space, input indexes grouped by table");
        for (Set<Index> set : indexesPerTable) {
            Rt.p("-------------");
            for (Index index : set) {
                allIndex.add(index);
                Rt.p(index);
            }
        }

        inumSpace = new HashSet<InumPlan>();

        hasPlanWhichDontUseIndex = false;
        ibg(statement, delegate, allIndex, inumSpace);
        // for (List<Index> atomic : cartesianProduct(indexesPerTable)) {
        // sqlPlan = delegate.explain(statement, new HashSet<Index>(atomic))
        // .getPlan();
        //
        // if (sqlPlan.contains(NLJ))
        // continue;
        //
        // templatePlan = new InumPlan(delegate, sqlPlan);
        //
        // if (isPlanUsingAllInterestingOrders(templatePlan, atomic))
        // inumSpace.add(templatePlan);
        // }

        // check if NLJ is considered
        // minimumAtomic = getMinimumAtomicConfiguration(new InumPlan(delegate,
        // delegate.explain(statement).getPlan()));
        //
        // sqlPlan = delegate
        // .explain(statement, new HashSet<Index>(minimumAtomic))
        // .getPlan();
        //
        // if (sqlPlan.contains(NLJ))
        // inumSpace.add(new InumPlan(delegate, sqlPlan));
        // timer.finishAndReset();
        Rt.p("Num of INUM template plans: "+ inumSpace.size());
        for (InumPlan plan : inumSpace) {
            Rt.p(plan.toString());
            Rt.p("Num of slots: "+plan.getSlots().size());
            for (TableAccessSlot slot : plan.getSlots()) {
                Rt.p(slot.getTable().toString()+" "+slot.getIndex().toString());
            }
        }
        Rt.p("Complete building INUM space");

        return inumSpace;
    }

    /**
     * Checks if the plan actually uses the indexes in the given interesting
     * order.
     * 
     * @param plan
     *            The plan returned by the optimizer
     * @param interestingOrders
     *            The given interesting order
     * @return {@code true} if the plan uses indexes corresponding to the given
     *         interesting orders; {@code false} otherwise
     */
    private static boolean isPlanUsingAllInterestingOrders(InumPlan plan,
            List<Index> interestingOrders) {
        TableAccessSlot slot;
        boolean isInterestingOrderFTS;
        boolean isSlotFTS;

        for (Index index : interestingOrders) {
            slot = plan.getSlot(index.getTable());
            isInterestingOrderFTS = index instanceof FullTableScanIndex;
            isSlotFTS = slot.getIndex() instanceof FullTableScanIndex;

            if (isInterestingOrderFTS && !isSlotFTS)
                throw new RuntimeException(
                        "interesting order is a FTS but the optimizer uses a materialized index");

            if (!isInterestingOrderFTS && isSlotFTS)
                // the given interesting order is an index, but the one returned
                // by the optimizer
                // is a FTS: not compatible
                return false;

            // if (isInterestingOrderFTS && isSlotInterestingOrderFTS)
            // this is fine, because both are full table scans

            if (!isInterestingOrderFTS && !isSlotFTS)
                // need to check whether the two indexes are the same
                if (!index.equalsContent(slot.getIndex()))
                    return false;
        }

        return true;
    }

    /**
     * Returns the minimum atomic configuration for the given plan. A minimum
     * index for a table and a statement is the index that covers all the
     * columns accessed by the statement of a given table. Thus, the minimum
     * atomic configuration is the set of covering indexes of a statement.
     * 
     * @param plan
     *            the plan returned by an optimizer
     * @return the atomic configuration for the statement associated with the
     *         plan
     * @throws SQLException
     *             if there's a table without a corresponding slot in the given
     *             inum plan
     */
    private static List<Index> getMinimumAtomicConfiguration(InumPlan plan)
            throws SQLException {
        List<Index> ios = new ArrayList<Index>();

        for (TableAccessSlot s : plan.getSlots()) {

            if (s.getColumnsFetched() == null)
                throw new SQLException("Can't find columns fetched for "
                        + s.getTable());

            ios.add(new InumInterestingOrder(s.getColumnsFetched()));
        }

        return ios;
    }

}
