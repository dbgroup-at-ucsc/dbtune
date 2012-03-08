package edu.ucsc.dbtune.util;

import java.sql.SQLException;

import java.util.HashSet;
import java.util.Set;

import edu.ucsc.dbtune.inum.FullTableScanIndex;
import edu.ucsc.dbtune.inum.InumInterestingOrder;

import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.metadata.Table;

import edu.ucsc.dbtune.optimizer.plan.InumPlan;
import edu.ucsc.dbtune.optimizer.plan.Operator;
import edu.ucsc.dbtune.optimizer.plan.SQLStatementPlan;
import edu.ucsc.dbtune.optimizer.plan.TableAccessSlot;


import static com.google.common.collect.Iterables.get;

import static edu.ucsc.dbtune.inum.FullTableScanIndex.getFullTableScanIndexInstance;

import static edu.ucsc.dbtune.optimizer.plan.Operator.SORT;
import static edu.ucsc.dbtune.optimizer.plan.Operator.SUBQUERY;
import static edu.ucsc.dbtune.optimizer.plan.Operator.TABLE_SCAN;
import static edu.ucsc.dbtune.optimizer.plan.Operator.TEMPORARY_TABLE_SCAN;

import static edu.ucsc.dbtune.util.MetadataUtils.getIndexesReferencingTables;

/**
 * @author Ivo Jimenez
 */
public final class InumUtils
{
    /**
     * utility class.
     */
    private InumUtils()
    {
    }

    /**
     * Completes a given configuration by adding the FTS index. A complete configuration is 
     * guaranteed to have at least one index for every table referenced in the INUM space statement, 
     * where the worst case is when the {@link FullTableScanIndex} is the only index for a 
     * particular table.
     *
     * @param inumSpace
     *      the set of template plans
     * @param configuration
     *      set of indexes that will be completed by adding FTS for each table referenced by the 
     *      inum space
     * @return
     *      a complete configuration
     * @throws SQLException
     *      if {@link #getFullTableScanIndexInstance} throws an exception
     */
    public static Set<Index> complete(Set<InumPlan> inumSpace, Set<? extends Index> configuration)
        throws SQLException
    {
        return complete(get(inumSpace, 0), configuration);
    }

    /**
     * Completes a given configuration by adding the FTS index. A complete configuration is 
     * guaranteed to have at least one index for every table referenced in the INUM space statement, 
     * where the worst case is when the {@link FullTableScanIndex} is the only index for a 
     * particular table.
     *
     * @param plan
     *      a representative plan of the INUM space
     * @param configuration
     *      set of indexes that will be completed by adding FTS for each table referenced by the 
     *      inum space
     * @return
     *      a complete configuration
     * @throws SQLException
     *      if {@link #getFullTableScanIndexInstance} throws an exception
     */
    public static Set<Index> complete(SQLStatementPlan plan, Set<? extends Index> configuration)
        throws SQLException
    {
        Set<Index> indexes = getIndexesReferencingTables(configuration, plan.getTables());

        for (Table table : plan.getTables())
            indexes.add(FullTableScanIndex.getFullTableScanIndexInstance(table));

        return indexes;
    }

    /**
     * Returns the minimum atomic configuration for the given plan. A minimum index for a table and 
     * a statement is the set of indexes that cover all the columns accessed by the statement of a 
     * given table. Thus, the minimum atomic configuration is the set of covering indexes of a 
     * statement.
     * 
     * @param plan
     *      the plan returned by an optimizer
     * @return
     *      the minimum atomic configuration for the statement associated with the plan
     * @throws SQLException
     *      if there's a table without a corresponding slot in the given inum plan
     */
    public static Set<Index> getMinimumAtomicConfiguration(InumPlan plan) throws SQLException
    {
        Set<Index> min = new HashSet<Index>();
        
        for (Table t : plan.getTables()) {

            TableAccessSlot s = plan.getSlot(t);
            
            if (s == null)
                throw new SQLException("No slot for " + t + " in " + plan);

            if (s.getColumnsFetched() == null)
                throw new SQLException("Can't find columns fetched for " + s.getTable());
            
            if (s.getColumnsFetched().size() == 0)
                min.add(getFullTableScanIndexInstance(t));
            else
                min.add(new InumInterestingOrder(s.getColumnsFetched()));
        }
        
        return min;
    }

    /**
     * Pre-processes a plan by (1) removing temporary table scans and (2) transforming non-leaf 
     * table scans.
     *
     * @param plan
     *      plan to be preprocessed
     * @throws SQLException
     *      if an error occurs while pre-processing
     */
    public static void preprocess(SQLStatementPlan plan) throws SQLException
    {
        while (removeTemporaryTables(plan))
            plan.getStatement();

        rewriteNonLeafTableScans(plan);
    }

    /**
     * Renames the first occurrence of a {@link TEMPORARY_TABLE_SCAN} leaf operator (to {@link 
     * SORT}) by calling {@link #renameClosestJoinAndRemoveBranchComingFrom}.
     *
     * @param plan
     *      plan whose non-leaf table scan operators are to be converted into leafs
     * @return
     *      {@code true} if a rename was performed; {@code false} otherwise
     * @throws SQLException
     *      if {@link #renameClosestJoinAndRemoveBranchComingFrom} throws an exception
     */
    private static boolean removeTemporaryTables(SQLStatementPlan plan) throws SQLException
    {
        boolean removed = false;

        for (Operator o : plan.toList()) {
            if (o.getName().equals(TEMPORARY_TABLE_SCAN) && plan.getChildren(o).isEmpty()) {
                renameClosestJoinAndRemoveBranchComingFrom(plan, o, SORT);
                removed = true;
                break;
            }
        }

        return removed;
    }

    /**
     * Renames the closest node in the three (bottom-up) that is an ascendant of {@code operator} 
     * and removes the entire branch that hangs from it (from the point of view of {@code 
     * operator}). For example, if a plan like the following is given:
     * <pre>
     * {@code .
     *
     *                 Rows 
     *                RETURN
     *                (   1)
     *                 Cost 
     *                  I/O 
     *                  |
     *                2824.22 
     *                NLJOIN
     *                (   2)
     *                211540 
     *                207998 
     *             /----+----\
     *         9320.87        0.303 
     *         TBSCAN        TBSCAN
     *         (   3)        (   4)
     *         211526       0.0103428 
     *         207998           0 
     *           |             |
     *       6.00122e+06        2 
     *     TABLE: TPCH       SORT  
     *        LINEITEM       (   5)
     *           Q3         0.0030171 
     *                          0 
     *                         |
     *                          2 
     *                  TEMPORARY_TABLE_SCAN
     *                       (   6)
     *                     2.95215e-05 
     *                          0 
     *                         |
     *                          2 
     * }
     * </pre>
     * <p>
     * If this method is invoked with {@code plan, genRowOperator, "NLJOIN", "SORT"}, the plan looks 
     * like the following when this method returns:
     * <pre>
     * {@code .
     *
     *                 Rows 
     *                RETURN
     *                (   1)
     *                 Cost 
     *                  I/O 
     *                  |
     *                2824.22 
     *                 SORT
     *                (   2)
     *                211540 
     *                207998 
     *                  |
     *                9320.87
     *                TBSCAN
     *                (   3)
     *                211526
     *                207998
     *                  |
     *              6.00122e+06
     *            TABLE: TPCH
     *               LINEITEM
     *                  Q3
     * }
     * </pre>
     *
     * @param plan
     *      plan that is modified
     * @param child
     *      node whose closest parent named {@code oldName} gets renamed. The branch that comes from 
     *      this node up to the parent is entirely removed
     * @param newName
     *      new name of the parent
     * @throws SQLException
     *      if a {@code operatorName} is not found in any ascendant of {@code child}
     */
    private static void renameClosestJoinAndRemoveBranchComingFrom(
            SQLStatementPlan plan, Operator child, String newName)
        throws SQLException
    {
        Operator ascendant = null;
        Operator shild = child;

        while ((ascendant = plan.getParent(shild)) != null) {

            if (ascendant.isJoin())
                break;

            shild = ascendant;
        }

        if (ascendant == null)
            throw new SQLException("Can't find closest join (ascendant) of " + child);

        plan.rename(ascendant, newName);
        plan.remove(shild);
    }

    /**
     * @param plan
     *      plan whose non-leaf table scan operators are to be converted into leafs
     * @throws SQLException
     *      if an error occurs while transforming the plan
     */
    private static void rewriteNonLeafTableScans(SQLStatementPlan plan) throws SQLException
    {
        Operator newSibling;
        Operator leaf;
        double leafCost;

        for (Operator o : plan.toList()) {

            if (o.getName().equals(TABLE_SCAN) && o.getDatabaseObjects().size() > 0)

                if (plan.getChildren(o).size() == 0) {
                    // fine, it is an actual leaf
                    plan.getChildren(o);
                } else if (plan.getChildren(o).size() == 1) {

                    newSibling = plan.getChildren(o).get(0);

                    leafCost = o.getAccumulatedCost() - newSibling.getAccumulatedCost();

                    leaf = new Operator(TABLE_SCAN, leafCost, o.getCardinality());

                    leaf.add(o.getDatabaseObjects().get(0));
                    leaf.addColumnsFetched(o.getColumnsFetched());
                    leaf.add(o.getPredicates());

                    plan.rename(o, SUBQUERY);
                    plan.removeDatabaseObject(o);
                    plan.removeColumnsFetched(o);
                    plan.removePredicates(o);
                    plan.setChild(o, leaf);
                } else {
                    throw new SQLException(
                        "Don't know how to handle " + TABLE_SCAN + " with more than one child");
                }
        }
    }
}
