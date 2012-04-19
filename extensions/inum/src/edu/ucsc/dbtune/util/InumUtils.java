package edu.ucsc.dbtune.util;

import java.sql.SQLException;

import java.util.HashSet;
import java.util.Set;

import edu.ucsc.dbtune.inum.DerbyInterestingOrdersExtractor;
import edu.ucsc.dbtune.inum.FullTableScanIndex;
import edu.ucsc.dbtune.inum.InumInterestingOrder;

import edu.ucsc.dbtune.metadata.Catalog;
import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.metadata.Table;

import edu.ucsc.dbtune.optimizer.plan.InumPlan;
import edu.ucsc.dbtune.optimizer.plan.Operator;
import edu.ucsc.dbtune.optimizer.plan.SQLStatementPlan;
import edu.ucsc.dbtune.optimizer.plan.TableAccessSlot;

import edu.ucsc.dbtune.workload.SQLStatement;

import static com.google.common.collect.Iterables.get;

import static edu.ucsc.dbtune.inum.DerbyInterestingOrdersExtractor.BoundSQLStatementParseTree;
import static edu.ucsc.dbtune.inum.FullTableScanIndex.getFullTableScanIndexInstance;

import static edu.ucsc.dbtune.optimizer.plan.Operator.FETCH;
import static edu.ucsc.dbtune.optimizer.plan.Operator.SORT;
import static edu.ucsc.dbtune.optimizer.plan.Operator.SUBQUERY;
import static edu.ucsc.dbtune.optimizer.plan.Operator.TABLE_SCAN;
import static edu.ucsc.dbtune.optimizer.plan.Operator.TEMPORARY_TABLE_SCAN;

import static edu.ucsc.dbtune.util.MetadataUtils.getIndexesReferencingTables;

import static edu.ucsc.dbtune.workload.SQLCategory.DELETE;
import static edu.ucsc.dbtune.workload.SQLCategory.INSERT;

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
     * Returns the covering atomic configuration for the given plan. A covering index for a table 
     * referenced in a statement covers all the columns accessed by it. Thus, the covering atomic 
     * configuration is the set of covering indexes for a statement.
     * 
     * @param plan
     *      the plan returned by an optimizer
     * @return
     *      the minimum atomic configuration for the statement associated with the plan
     * @throws SQLException
     *      if there's a table without a corresponding slot in the given inum plan
     */
    public static Set<Index> getCoveringAtomicConfiguration(InumPlan plan) throws SQLException
    {
        Set<Index> min = new HashSet<Index>();
        
        for (Table t : plan.getTables())
            if (plan.getSlot(t) == null)
                throw new SQLException("No slot for " + t + " in " + plan);
            else
                min.add(getCoveringIndex(plan.getSlot(t)));
        
        return min;
    }

    /**
     * @param slot
     *      slot for which a covering index is created
     * @return
     *      the index that covers the columns accessed by the slot
     * @throws SQLException
     *      if the FTS for the slot doesn't exist
     */
    public static InumInterestingOrder getCoveringIndex(TableAccessSlot slot) throws SQLException
    {
        if (slot.getColumnsFetched() == null)
            throw new SQLException("Can't find columns fetched for " + slot.getTable());

        if (slot.getColumnsFetched().size() == 0)
            return getFullTableScanIndexInstance(slot.getTable());

        return new InumInterestingOrder(slot.getColumnsFetched());
    }

    /**
     * Returns the maximum atomic configuration for the statement. The maximum configuration is the 
     * set of indexes that contain no attributes other than the join columns used on the statement.
     * 
     * @param statement
     *      SQL statement for which the INUM space is computed
     * @param catalog
     *      used to retrieve metadata for objects referenced in the statement
     * @return
     *      the maximum atomic configuration for the statement associated with the plan
     * @throws SQLException
     *      if there's a table without a corresponding slot in the given inum plan
     */
    public static Set<Index> getMaximumAtomicConfiguration(
            SQLStatement statement, Catalog catalog)
        throws SQLException
    {
        Set<InumInterestingOrder> ios = extractInterestingOrdersFromJoinPredicates(statement, 
                catalog);
        Set<Index> max = new HashSet<Index>();

        for (InumInterestingOrder i : ios)
            max.add(i);

        return max;
    }

    /**
     * Extracts interesting orders through the use of the {@link DerbyInterestingOrdersExtractor}.
     * 
     * @param statement
     *      SQL statement for which the INUM space is computed
     * @param catalog
     *      used to retrieve metadata for objects referenced in the statement
     * @return
     *      the maximum atomic configuration for the statement associated with the plan
     * @throws SQLException
     *      if there's a table without a corresponding slot in the given inum plan
     */
    public static Set<InumInterestingOrder> extractInterestingOrders(
            SQLStatement statement, Catalog catalog)
        throws SQLException
    {
        return new DerbyInterestingOrdersExtractor(catalog).extract(statement);
    }

    /**
     * Extracts interesting orders through the use of the {@link DerbyInterestingOrdersExtractor}.
     * 
     * @param statement
     *      SQL statement for which the INUM space is computed
     * @param catalog
     *      used to retrieve metadata for objects referenced in the statement
     * @return
     *      the maximum atomic configuration for the statement associated with the plan
     * @throws SQLException
     *      if there's a table without a corresponding slot in the given inum plan
     */
    public static Set<InumInterestingOrder> extractInterestingOrdersFromJoinPredicates(
            SQLStatement statement, Catalog catalog)
        throws SQLException
    {
        DerbyInterestingOrdersExtractor ioExtractor;
        BoundSQLStatementParseTree parseTree;

        ioExtractor = new DerbyInterestingOrdersExtractor(catalog);
        parseTree = new BoundSQLStatementParseTree(ioExtractor.getParseTree(statement), catalog);

        Set<InumInterestingOrder> ios = new HashSet<InumInterestingOrder>();

        ioExtractor.extractFromJoinPredicates(parseTree, ios);

        for (BoundSQLStatementParseTree subquery : parseTree.getBoundSubqueries())
            ioExtractor.extractFromJoinPredicates(subquery, ios);

        return ios;
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
    public static boolean removeTemporaryTables(SQLStatementPlan plan) throws SQLException
    {
        boolean removed = false;

        for (Operator o : plan.leafs()) {

            if (plan.getStatement().getSQLCategory().isSame(INSERT) ||
                    plan.getStatement().getSQLCategory().isSame(DELETE)) {
                // no need to look for a join
                plan.remove(o);
                break;
            }

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
            throw new SQLException(
                "In\n" + plan + "\n\nCan't find closest join (ascendant) of " + child);

        plan.rename(ascendant, newName);
        plan.remove(shild);
    }

    /**
     * In some cases a plan might have a base-table access operator ({@link INDEX_SCAN} or {@link 
     * TABLE_SCAN}) that is not a leaf. For example, in DB2, the following query (TPC-H, query 16):
     * <p>
     * <pre>
     * {@code .
     *
     * select
     *     p_brand,
     *     p_type,
     *     p_size,
     *     count(distinct ps_suppkey) as supplier_cnt
     * from
     *     tpch.partsupp,
     *     tpch.part
     * where
     *     p_partkey = ps_partkey
     *     and p_brand &lt;&gt; 'Brand#41'
     *     and p_type not like 'MEDIUM BURNISHED%'
     *     and p_size in (4, 21, 15, 41, 49, 43, 27, 47)
     *     and ps_suppkey not in (
     *         select
     *             s_suppkey
     *         from
     *             tpch.supplier
     *         where
     *             s_comment like '%Customer%Complaints%'
     *     )
     * group by
     *     p_brand,
     *     p_type,
     *     p_size
     * order by
     *     supplier_cnt desc,
     *     p_brand,
     *     p_type,
     *     p_size;
     * 
     *
     * }
     * </pre>
     * <p>
     * Generates the following plan:
     * <p>
     * <pre>
     * {@code .
     *
     *                       HSJOIN
     *                       (   7)
     *                      1.193e+06 
     *                        41538 
     *               /---------+---------\
     *           400000                  30506.7 
     *           TBSCAN                  TBSCAN
     *           (   8)                  (  12)
     *         1.17329e+06               7750.16 
     *            32343                   7617 
     *         /---+----\                  |
     *     599.479      800000           200000 
     *     TBSCAN   TABLE: TPCH      TABLE: TPCH    
     *     (   9)      PARTSUPP           PART
     *     419.594        Q6               Q5
     *       410 
     *       |
     *     599.479 
     *     SORT  
     *     (  10)
     *     419.564 
     *       410 
     *       |
     *     599.479 
     *     TBSCAN
     *     (  11)
     *     419.373 
     *       410 
     *       |
     *      10000 
     * TABLE: TPCH    
     *    SUPPLIER
     *       Q3
     * }
     * </pre>
     * <p>
     * The {@code TBSCAN} (id=8) operator represents the subquery that is applied to the
     * {@code SUPPLIER} relation, for each distinct value of {@code ps_suppkey}.
     * <p>
     * This method renames the non-leaf node to {@link #SUBQUERY} and creates a new leaf node that 
     * corresponds to the scan of the table. The {@link #SUBQUERY} operator can be thought as a kind 
     * of single-scan , i.e. both children of {@code SUBQUERY} are scanned once.
     * <pre>
     * {@code .
     *
     *                       HSJOIN
     *                       (   7)
     *                      1.193e+06 
     *                        41538 
     *               /---------+---------\
     *           400000                  30506.7 
     *           SUBQUERY                TBSCAN
     *           (   8)                  (  12)
     *         1.17329e+06               7750.16 
     *            32343                   7617 
     *         /---+----\                  |
     *     599.479     400000             200000 
     *     TBSCAN      TBSCAN         TABLE: TPCH    
     *     (   9)      (   13)              PART
     *     419.594   1.17329e+06            Q5
     *       410        32343     
     *       |            |
     *     599.47      800000    
     *     SORT      TABLE: TPCH 
     *     (  10)     PARTSUPP 
     *     419.564       Q6   
     *       410 
     *       |
     *     599.479 
     *     TBSCAN
     *     (  11)
     *     419.373 
     *       410 
     *       |
     *      10000 
     * TABLE: TPCH    
     *    SUPPLIER
     *       Q3
     * }
     * </pre>
     * <p>
     * This method operates in a bottom up approach and returns immediately after it performs the 
     * first rewrite. This with the purpose of avoiding concurrent modification of the members of 
     * the plan
     *
     * @param plan
     *      plan whose non-leaf table access operators are to be converted into leafs
     * @return
     *      {@code true} if the plan was modified; {@code false} otherwise
     * @throws SQLException
     *      if an non-leaf data access operator has more than one children
     */
    public static boolean rewriteNonLeafDataAccessOperators(SQLStatementPlan plan)
        throws SQLException
    {
        for (Operator o : plan.leafs()) {
            
            Operator parent;
            Operator child = o;

            while ((parent = plan.getParent(child)) != null) {

                if (child.isDataAccess() && !child.equals(FETCH)) {

                    Operator newSibling;
                    Operator leaf;
                    double leafCost;

                    if (plan.getChildren(child).size() == 1) {

                        newSibling = plan.getChildren(child).get(0);

                        leafCost = child.getAccumulatedCost() - newSibling.getAccumulatedCost();

                        leaf = new Operator(child.getName(), leafCost, child.getCardinality());

                        leaf.add(child.getDatabaseObjects().get(0));
                        leaf.addColumnsFetched(child.getColumnsFetched());
                        leaf.add(child.getPredicates());

                        plan.rename(child, SUBQUERY);
                        plan.removeDatabaseObject(child);
                        plan.removeColumnsFetched(child);
                        plan.removePredicates(child);
                        plan.setChild(child, leaf);

                        return true;
                    } else if (plan.getChildren(child).size() > 1) {
                        throw new SQLException(
                            "Don't know how to handle " + TABLE_SCAN + " with more than one child");
                    }
                    // else {} // is fine, it is an actual leaf
                }

                child = parent;
            }

        }

        return false;
    }

    /**
     * Instantiates an operator for an unseen index.
    protected Operator instantiateOperatorForUnseenIndex(SQLStatement sql, Index index)
        throws SQLException
    {
        SQLStatementPlan plan = delegate.explain(sql, Sets.<Index>newHashSet(index)).getPlan();

        Operator indexScan = null;
        Operator fetch = null;
        Operator otherFetch = null;

        for (Operator o : plan.toList()) {

            if (!o.getName().equals(INDEX_SCAN))
                continue;

            if (o.getDatabaseObjects().size() != 1)
                throw new SQLException(INDEX_SCAN + " should be referring to a DB object");

            DatabaseObject dbo = o.getDatabaseObjects().get(0);

            if (dbo instanceof Index && index.getTable().equals(((Index) dbo).getTable())) {

                if (indexScan != null) {
                    // it is possible to find more than one INDEX_SCAN that refers {@code index}, 
                    // since some optimizers scan the same index more than once. As long as the 
                    // INDEX_SCANs are all referring to the SAME index, it's OK
                    if (!index.equals(dbo)) {
                        throw new SQLException(
                            "Other " + INDEX_SCAN + " for " + index.getTable() + " in " + plan);
                    } else {
                        // check that they're sharing the same FETCH parent, otherwise 
                        // extractCostOfLeafAndRemoveFetch won't return the appropriate costh
                        otherFetch = plan.findAncestorWithName(o, FETCH);

                        if ((fetch != null && otherFetch == null) ||
                                (fetch == null && otherFetch != null) ||
                                (fetch != null && otherFetch != null && !fetch.equals(otherFetch)))
                            throw new SQLException("Haven't implemented this case yet");

                        dbo.getName();
                    }
                } else {
                    indexScan = o;
                    fetch = plan.findAncestorWithName(o, FETCH);
                }
            }
        }

        if (indexScan == null)
            // plan is not using the index, so the index is not compatible with this slot
            return INCOMPATIBLE;

        double newIndexScanCost = 0;
        double costOfChildren = 0;

        if (plan.getChildren(indexScan).size() > 0) {
            // check if INDEX_SCAN has children and remove them. Yes, it is
            // possible for INDEX_SCAN to have children.
            for (Operator child : plan.getChildren(indexScan)) {
                costOfChildren += child.getAccumulatedCost();
                plan.remove(child);
            }
        }

        newIndexScanCost = extractCostOfLeafAndRemoveFetch(plan, indexScan) - costOfChildren;

        Operator newIndexScan = new Operator(indexScan);

        newIndexScan.setAccumulatedCost(newIndexScanCost);

        return newIndexScan;
    }
     */
}
