package edu.ucsc.dbtune.inum;

import java.sql.SQLException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import edu.ucsc.dbtune.metadata.Catalog;
import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.metadata.Table;
import edu.ucsc.dbtune.optimizer.ExplainedSQLStatement;
import edu.ucsc.dbtune.optimizer.Optimizer;
import edu.ucsc.dbtune.optimizer.plan.InumPlan;
import edu.ucsc.dbtune.optimizer.plan.SQLStatementPlan;
import edu.ucsc.dbtune.workload.SQLStatement;

import static com.google.common.collect.Iterables.get;

import static edu.ucsc.dbtune.inum.FullTableScanIndex.getFullTableScanIndexInstance;
import static edu.ucsc.dbtune.optimizer.plan.Operator.NLJ;
import static edu.ucsc.dbtune.util.InumUtils.extractInterestingOrders;
import static edu.ucsc.dbtune.util.InumUtils.getCoveringAtomicConfiguration;
import static edu.ucsc.dbtune.util.MetadataUtils.getIndexesReferencingTable;

/**
 * Common functionality for space computation.
 *
 * @author Ivo Jimenez
 */
public abstract class AbstractSpaceComputation implements InumSpaceComputation
{
    private static Set<Index> empty = new HashSet<Index>();

    /**
     * Computes the INUM space by extracting interesting orders using the {@link 
     * DerbyInterestingOrdersExtractor}. The configuration associated to each table is completed 
     * using {@link #complete}, i.e. this method ensures that each table has at least the {@link 
     * FullTableScanIndex} interesting order in the set of interesting orders. After this is done, 
     * the {@link #computeWithCompleteConfiguration} method is called.
     *
     * @param space
     *      the space to be computed. {@link Set#clear} is invoked before populating it.
     * @param statement
     *      SQL statement for which the INUM space is computed
     * @param delegate
     *      optimizer used to execute {@link Optimizer#explain(SQLStatement, Configuration) what if 
     *      calls}
     * @param catalog
     *      used to retrieve metadata for objects referenced in the statement
     * @throws SQLException
     *      if the inum space can't be populated
     */
    @Override
    public void compute(
            Set<InumPlan> space, SQLStatement statement, Optimizer delegate, Catalog catalog)
        throws SQLException
    {
        space.clear();

        // add FTS-on-all-slots template
        InumPlan templateForEmpty = new InumPlan(delegate, delegate.explain(statement, empty));

        space.add(templateForEmpty);

        // obtain plans for all the extracted interesting orders
        computeWithCompleteConfiguration(
                space, extractInterestingOrders(statement, catalog), statement, delegate);

        // NLJ heuristic referred in the INUM paper
        Set<Index> covering = getCoveringAtomicConfiguration(templateForEmpty);

        ExplainedSQLStatement coveringExplainedStmt = delegate.explain(statement, covering);

        if (coveringExplainedStmt.getPlan().contains(NLJ))
            space.add(new InumPlan(delegate, coveringExplainedStmt));
    }

    /**
     * Computes the space given a complete configuration that is extracted from a set of interesting 
     * orders. A complete configuration is guaranteed to have at least one index for every table 
     * referenced in the statement, where the worst case is when the {@link FullTableScanIndex} is 
     * the only index for a particular table.
     *
     * @param space
     *      the space to be computed. {@link Set#clear} is invoked before populating it.
     * @param indexes
     *      interesting orders
     * @param statement
     *      SQL statement for which the INUM space is computed
     * @param delegate
     *      optimizer used to execute {@link Optimizer#explain(SQLStatement, Configuration) what if 
     *      calls}
     * @throws SQLException
     *      if the inum space can't be populated
     */
    public abstract void computeWithCompleteConfiguration(
            Set<InumPlan> space,
            Set<? extends Index> indexes,
            SQLStatement statement,
            Optimizer delegate)
        throws SQLException;

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
    public static boolean isUsingAllInterestingOrders(
            SQLStatementPlan plan, Collection<Index> interestingOrders)
        throws SQLException
    {
        for (Table table : plan.getTables()) {

            Index indexFromPlan = getIndexReferencingTable(plan.getIndexes(), table);
            Index indexFromOrder = getIndexReferencingTable(interestingOrders, table);

            boolean isIndexFromPlanFTS = indexFromPlan instanceof FullTableScanIndex;
            boolean isIndexFromOrderFTS = indexFromOrder instanceof FullTableScanIndex;
            
            if (isIndexFromOrderFTS && isIndexFromPlanFTS)
                // this is fine
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
    public static Index getIndexReferencingTable(Collection<Index> indexes, Table table)
        throws SQLException
    {
        Set<Index> indexesReferncingTable = getIndexesReferencingTable(indexes, table);

        if (indexesReferncingTable.size() > 1)
            throw new SQLException("More than one index on slot for " + table);

        if (indexesReferncingTable.size() == 0)
            return getFullTableScanIndexInstance(table);

        return get(indexesReferncingTable, 0);
    }
}
