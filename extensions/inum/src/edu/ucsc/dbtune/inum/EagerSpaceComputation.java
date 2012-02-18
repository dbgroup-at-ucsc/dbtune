package edu.ucsc.dbtune.inum;

import java.sql.SQLException;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import edu.ucsc.dbtune.metadata.Catalog;
import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.optimizer.Optimizer;
import edu.ucsc.dbtune.optimizer.plan.InumPlan;
import edu.ucsc.dbtune.optimizer.plan.SQLStatementPlan;
import edu.ucsc.dbtune.optimizer.plan.TableAccessSlot;
import edu.ucsc.dbtune.workload.SQLStatement;

import static com.google.common.collect.Sets.cartesianProduct;
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
public class EagerSpaceComputation implements InumSpaceComputation
{
    /**
     * {@inheritDoc}
     */
    @Override
    public void compute(
            Set<InumPlan> space, SQLStatement statement, Optimizer delegate, Catalog catalog)
        throws SQLException
    {
        List<Set<Index>> indexesPerTable;
        List<Index> minimumAtomic;
        DerbyInterestingOrdersExtractor interestingOrdersExtractor;
        SQLStatementPlan sqlPlan;
        InumPlan templatePlan;

        interestingOrdersExtractor = new DerbyInterestingOrdersExtractor(catalog, true);
        indexesPerTable = interestingOrdersExtractor.extract(statement);

        space.clear();

        for (List<Index> atomic : cartesianProduct(indexesPerTable)) {
            sqlPlan = delegate.explain(statement, new HashSet<Index>(atomic)).getPlan();
            
            if (sqlPlan.contains(NLJ) && !isAllFTS(atomic))
                continue;
            
            templatePlan = new InumPlan(delegate, sqlPlan);

            if (isUsingAllInterestingOrders(templatePlan, atomic))
                space.add(templatePlan);
        }
        
        // check if NLJ is considered 
        minimumAtomic =
            getMinimumAtomicConfiguration(
                    new InumPlan(delegate, delegate.explain(statement).getPlan()));
        
        sqlPlan = delegate.explain(statement, new HashSet<Index>(minimumAtomic)).getPlan();

        if (sqlPlan.contains(NLJ))
            space.add(new InumPlan(delegate, sqlPlan));
    }
    
    /**
     * Checks if the plan actually uses the given {@code interestingOrders}. For each slot, the 
     * corresponding index is retrieved and compares it against the corresponding index contained in 
     * {@code interestingOrders}, if for any slot the order doesn't correspond to the used 
     * interesting order, the method returns {@code false}.
     * 
     * @param plan
     *      The plan returned by the optimizer
     * @param interestingOrders
     *      The given interesting order
     * @return
     *      {@code true} if the plan uses all the indexes corresponding to the given interesting 
     *      orders; {@code false} otherwise
     * @throws SQLException
     *      if one slot if {@code null}; if the plan is using a materialized index
     */
    private static boolean isUsingAllInterestingOrders(InumPlan plan, List<Index> interestingOrders)
        throws SQLException
    {
        TableAccessSlot slot;
        boolean isInterestingOrderFTS;
        boolean isSlotFTS;

        for (Index index : interestingOrders) {

            slot = plan.getSlot(index.getTable());

            if (slot == null)
                throw new SQLException("No slot for table " + index.getTable() + " in \n" + plan);

            isInterestingOrderFTS = index instanceof FullTableScanIndex;
            isSlotFTS = slot.getIndex() instanceof FullTableScanIndex;
            
            // if (isInterestingOrderFTS && isSlotInterestingOrderFTS)
                // this is fine, because both are full table scans
            
            if (isInterestingOrderFTS && !isSlotFTS)
                throw new SQLException(
                    "Interesting order is a FTS but the optimizer uses a materialized index");
            
            if (!isInterestingOrderFTS && isSlotFTS)
                // the given interesting order is an actual index, but the one returned by the 
                // optimizer is the FTS index, thus they're not compatible
                return false;
            
            if (!isInterestingOrderFTS && !isSlotFTS)
                // need to check whether the two indexes are the same
                if (!index.equalsContent(slot.getIndex()))
                    return false;
        }
        
        return true;
    }

    /**
     * Returns the minimum atomic configuration for the given plan. A minimum index for a table and 
     * a statement is the index that covers all the columns accessed by the statement of a given 
     * table. Thus, the minimum atomic configuration is the set of covering indexes of a statement.
     * 
     * @param plan
     *      the plan returned by an optimizer
     * @return
     *      the atomic configuration for the statement associated with the plan
     * @throws SQLException
     *      if there's a table without a corresponding slot in the given inum plan
     */
    private static List<Index> getMinimumAtomicConfiguration(InumPlan plan) throws SQLException
    {
        List<Index> ios = new ArrayList<Index>();
        
        for (TableAccessSlot s : plan.getSlots()) {
            
            if (s.getColumnsFetched() == null)
                throw new SQLException("Can't find columns fetched for " + s.getTable());
            
            ios.add(new InumInterestingOrder(s.getColumnsFetched()));
        }
        
        return ios;
    }

    /**
     * Checks if a given collection of indexes contains only {@link FullTableScanIndex} instances.
     *
     * @param indexes
     *      collection of indexes that is being checked
     * @return
     *      {@code true} if all indexes are instances of {@link FullTableScanIndex}; {@code false} 
     *      otherwise
     */
    private static boolean isAllFTS(Collection<Index> indexes)
    {
        for (Index i : indexes)
            if (!(i instanceof FullTableScanIndex))
                return false;

        return true;
    }
}
