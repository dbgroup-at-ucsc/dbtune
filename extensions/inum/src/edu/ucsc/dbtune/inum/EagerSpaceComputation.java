package edu.ucsc.dbtune.inum;

import java.sql.SQLException;

import java.util.ArrayList;
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
    public Set<InumPlan> compute(SQLStatement statement, Optimizer delegate, Catalog catalog)
        throws SQLException
    {
        List<Set<Index>> indexesPerTable;
        Set<InumPlan> inumSpace;
        List<Index> minimumAtomic;
        DerbyInterestingOrdersExtractor interestingOrdersExtractor;
        SQLStatementPlan sqlPlan;
        InumPlan templatePlan;

        interestingOrdersExtractor = new DerbyInterestingOrdersExtractor(catalog, true);
        indexesPerTable = interestingOrdersExtractor.extract(statement);

        inumSpace = new HashSet<InumPlan>();

        for (List<Index> atomic : cartesianProduct(indexesPerTable)) {
            sqlPlan = delegate.explain(statement, new HashSet<Index>(atomic)).getPlan();

            if (sqlPlan.contains(NLJ))
                // ignore it since we check for NLJ below
                continue;
            
            templatePlan = new InumPlan(delegate, sqlPlan);

            if (isPlanUsingInterestingOrder(templatePlan, atomic))
                inumSpace.add(templatePlan);
        }
        
        minimumAtomic =
            getMinimumAtomicConfiguration(
                    new InumPlan(delegate, delegate.explain(statement).getPlan()));
        
        sqlPlan = delegate.explain(statement, new HashSet<Index>(minimumAtomic)).getPlan();

        if (sqlPlan.contains(NLJ))
            inumSpace.add(new InumPlan(delegate, sqlPlan));
        
        return inumSpace;
    }
    
    /**
     * Checks if the plan actually uses the indexes in the given interesting order.
     * 
     * @param plan
     *      The plan returned by the optimizer
     * @param interestingOrders
     *      The given interesting order
     * @return
     *      {@code true} if the plan uses the given interesting order, {@code false} otherwise
     */
    private static boolean isPlanUsingInterestingOrder(InumPlan plan, List<Index> interestingOrders)
    {
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
                // the given interesting order is an index, but the one returned by the optimizer
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
            
            ios.add(s.getColumnsFetched());
        }
        
        return ios;
    }
}
