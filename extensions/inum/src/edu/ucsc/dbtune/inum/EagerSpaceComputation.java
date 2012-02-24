package edu.ucsc.dbtune.inum;

import java.sql.SQLException;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;

import edu.ucsc.dbtune.metadata.Catalog;
import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.metadata.Table;
import edu.ucsc.dbtune.optimizer.Optimizer;
import edu.ucsc.dbtune.optimizer.plan.InumPlan;
import edu.ucsc.dbtune.optimizer.plan.SQLStatementPlan;
import edu.ucsc.dbtune.optimizer.plan.TableAccessSlot;
import edu.ucsc.dbtune.workload.SQLStatement;

import static com.google.common.collect.Iterables.get;
import static com.google.common.collect.Sets.cartesianProduct;
import static com.google.common.collect.Sets.newHashSet;

import static edu.ucsc.dbtune.optimizer.plan.Operator.NLJ;
import static edu.ucsc.dbtune.util.MetadataUtils.getIndexesReferencingTable;

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
    private static Set<Index> empty = newHashSet();

    /**
     * {@inheritDoc}
     */
    @Override
    public void compute(
            Set<InumPlan> space, SQLStatement statement, Optimizer delegate, Catalog catalog)
        throws SQLException
    {
        SQLStatementPlan sqlPlan;
        InumPlan templateForEmpty;

        List<Set<Index>> indexesPerTable =
            (new DerbyInterestingOrdersExtractor(catalog, true)).extract(statement);

        space.clear();

        for (List<Index> atomic : cartesianProduct(indexesPerTable)) {

            //System.out.println("\n   Explaining interesting order:\n       " + atomic + "\n");

            sqlPlan = delegate.explain(statement, newHashSet(atomic)).getPlan();

            //System.out.println("\n   Plan for interesting order:\n" + sqlPlan);

            if (sqlPlan.contains(NLJ)) {
                //System.out.println("\n   Uses NLJ; skipping\n");
                continue;
            }

            if (!isUsingAllInterestingOrders(sqlPlan, atomic)) {
                //System.out.println("\n   Not using all interesting orders sent; skipping\n");
                continue;
            }

            //System.out.println(
                //"\n   Adding template plan\n " + new InumPlan(delegate, sqlPlan) +
                //"\n   >>>>>>>\n");

            space.add(new InumPlan(delegate, sqlPlan));
        }

        templateForEmpty = new InumPlan(delegate, delegate.explain(statement, empty).getPlan());

        // check worst-case: empty inumspace, in which case we store the FTS-on-all-slots template
        if (space.isEmpty()) {
            //System.out.println("\n   INUM space empty; adding FTS-on-all-slots template\n");

            space.add(templateForEmpty);

            //System.out.println(
                    //"\n   Template for FTS-on-all-slots: \n" + new InumPlan(delegate, sqlPlan));
        }
        
        // check NLJ
        List<Index> minimumAtomic = getMinimumAtomicConfiguration(templateForEmpty);
        
        //System.out.println("\n   Checking for NLJ with minimum:\n      " + minimumAtomic + "\n");

        sqlPlan = delegate.explain(statement, newHashSet(minimumAtomic)).getPlan();

        //System.out.println("\n   Plan for NLJ and minimum atomic:\n" + sqlPlan + "\n");

        if (sqlPlan.contains(NLJ)) {
            //System.out.println("\n   Plan uses NLJ, creating template\n");
            //System.out.println(
                    //"\n   Template for NLJ: \n" + new InumPlan(delegate, sqlPlan) + "\n");

            space.add(new InumPlan(delegate, sqlPlan));
        } else {
            //System.out.println("\n   Plan DOESN'T use NLJ; skipping\n");
        }
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
     *      if a leaf of the plan corresponding to a table contains more than one index assigned to 
     *      it, i.e. if the plan is not atomic; if the plan is using a materialized index
     */
    private static boolean isUsingAllInterestingOrders(
            SQLStatementPlan plan, List<Index> interestingOrders)
        throws SQLException
    {
        for (Table table : plan.getTables()) {

            Index indexFromPlan = getIndexReferencingTable(plan.getIndexes(), table);
            Index indexFromOrder = getIndexReferencingTable(interestingOrders, table);

            boolean isIndexFromPlanFTS = indexFromPlan instanceof FullTableScanIndex;
            boolean isIndexFromOrderFTS = indexFromOrder instanceof FullTableScanIndex;
            
            if (isIndexFromOrderFTS && isIndexFromPlanFTS)
                // this is fine, because both are full table scans
                continue;
            
            if (isIndexFromOrderFTS && !isIndexFromPlanFTS)
                throw new SQLException(
                    "Interesting order is FTS but optimizer is using a materialized index");
            
            if (!isIndexFromOrderFTS && isIndexFromPlanFTS)
                // the given interesting order is an actual index, but the one returned by the 
                // optimizer is the FTS index, thus they're not compatible
                return false;
            
            if (!isIndexFromOrderFTS && !isIndexFromPlanFTS)
                // need to check whether the two indexes are the same
                if (!indexFromOrder.equalsContent(indexFromPlan))
                    return false;
        }
        
        return true;
    }

    /**
     * Returns the index that references the given table. If there is no index referencing the given 
     * table, the corresponding {@link FullTableScanIndex} is returned.
     *
     * @param indexes
     *      indexes that are iterated in order to look for one referencing to {@code table}
     * @param table
     *      table that should be referenced by an index
     * @return
     *      the index in {@code indexes} that refers to {@code table}; the {@link 
     *      FullTableScanIndex} if none is referring to {@code table}
     * @throws SQLException
     *      if a table is referenced by more than one index
     */
    private static Index getIndexReferencingTable(Collection<Index> indexes, Table table)
        throws SQLException
    {
        Set<Index> indexesReferncingTable = getIndexesReferencingTable(indexes, table);

        if (indexesReferncingTable.size() > 1)
            throw new SQLException("More than one index for slot for " + table);

        if (indexesReferncingTable.size() == 0)
            return FullTableScanIndex.getFullTableScanIndexInstance(table);

        return get(indexesReferncingTable, 0);
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
    public static List<Index> getMinimumAtomicConfiguration(InumPlan plan) throws SQLException
    {
        List<Index> ios = new ArrayList<Index>();
        
        for (TableAccessSlot s : plan.getSlots()) {
            
            if (s.getColumnsFetched() == null)
                throw new SQLException("Can't find columns fetched for " + s.getTable());
            
            ios.add(new InumInterestingOrder(s.getColumnsFetched()));
        }
        
        return ios;
    }
}
