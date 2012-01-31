package edu.ucsc.dbtune.inum;

import java.sql.SQLException;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import edu.ucsc.dbtune.metadata.Catalog;
import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.optimizer.Optimizer;
import edu.ucsc.dbtune.optimizer.plan.InumPlan;
import edu.ucsc.dbtune.optimizer.plan.TableAccessSlot;
import edu.ucsc.dbtune.workload.SQLStatement;

import static com.google.common.collect.Sets.cartesianProduct;

/**
 * An implementation of the INUM space computation that populates it eagerly without any kind of 
 * optimization. This is in contrast to other more sophisticated strategies such as the ones 
 * outlined in [1], like <i>Lazy</i> and <i>Cost-based</i> evaluation.
 * <p>
 * This implementation assumes that the plans obtained by the delegate don't contain NLJ operators.
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
            InumPlan plan = new InumPlan(delegate,
                    delegate.explain(statement, new HashSet<Index>(atomic)).getPlan());
            if (isPlanUsingInterestingOrder(plan, atomic)) 
                plans.add(plan);
        }

        return plans;
    }
    
    /**
     * Check if the plan actually uses the indexes in the given interesting order
     * 
     * @param plan
     *      The plan returned by the optimizer
     * @param interestingOrders
     *      The given interesting order
     * @return
     *      {@code true} if the plan uses the given interesting order,
     *      {@code false} otherwise
     */
    private boolean isPlanUsingInterestingOrder(InumPlan plan, List<Index> interestingOrders)
    {   
        TableAccessSlot slot; 
        for (Index index : interestingOrders) {
            slot = plan.getSlot(index.getTable());
            boolean isInterestingOrderFTS = (index instanceof FullTableScanIndex);
            boolean isSlotFTS = (slot.getIndex() instanceof FullTableScanIndex);
            
            if (isInterestingOrderFTS == true && isSlotFTS == false) 
                throw new RuntimeException("NOT yet handle the case of interesting order is a FTS " +
                		"but the optimizer uses a materialized index. ");
            
            // the given interesting order is an index, but the one returned by the optimizer
            // is a FTS: not compatible
            if (isInterestingOrderFTS == false && isSlotFTS == true)
                return false;
            
            // if (isInterestingOrderFTS == true && isSlotInterestingOrderFTS == true)
            // this is fine, because both are full table scans
            
            // need to check whether the two indexes are the same
            if (isInterestingOrderFTS == false && isSlotFTS == false)                
                if (index.equalsContent(slot.getIndex()) == false) 
                    return false;
        }
        
        return true;
    }
}
