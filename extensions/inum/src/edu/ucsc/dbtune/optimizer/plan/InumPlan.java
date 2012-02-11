package edu.ucsc.dbtune.optimizer.plan;

import java.sql.SQLException;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;

import edu.ucsc.dbtune.inum.FullTableScanIndex;

import edu.ucsc.dbtune.metadata.Column;
import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.metadata.Table;
import edu.ucsc.dbtune.optimizer.Optimizer;
import edu.ucsc.dbtune.workload.SQLStatement;

import static edu.ucsc.dbtune.util.MetadataUtils.getReferencedTables;

/**
 * Extends {@link SQLStatementPlan} class in order to add INUM-specific functionality. Specifically,
 * the capacity of returning the cost of the internal subplan, i.e. the cost of the original plan
 * minus the cost of the leafs. Also, adds the ability of obtaining the cost of a slot by plugging
 * an index into it.
 * <p>
 * For the case where the FTS is being proved, a {@link edu.ucsc.dbtune.metadata.IndexFullTableScan
 * special Index type} is used.
 *
 * @author Ivo Jimenez
 * @author Quoc Trung Tran
 * @see edu.ucsc.dbtune.metadata.IndexFullTableScan
 */
public class InumPlan extends SQLStatementPlan
{
    /**
     * The cost of the internal nodes of a plan; the paper refers to it as {@latex.inline
     * $\\beta_k$}.
     */
    private double internalPlanCost;

    /**
     * Convenience object that maps tables to slots. This is done so that the {@link
     * #getAccessSlots} and {@link #plugIntoSlots} method can run efficiently
     */
    private Map<Table, TableAccessSlot> slots;

    /**
     * We use this optimizer for doing what-if calls to compute index access cost.
     */
    private Optimizer delegate;

    /**
     * Creates a new instance of an INUM plan.
     *
     * @param delegate
     *      to do what-if optimization later on
     * @param plan
     *      a regular execution plan produced by a Optimizer implementations (through the {@link
     *      edu.ucsc.dbtune.optimizer.Optimizer#explain} or {@link
     *      edu.ucsc.dbtune.optimizer.PreparedSQLStatement#explain} methods)
     * @throws SQLException
     *      if the plan is referencing a table more than once or if the plan contains NLJ operators.
     */
    public InumPlan(Optimizer delegate, SQLStatementPlan plan) throws SQLException
    {
        super(plan);

        this.delegate = delegate;

        TableAccessSlot slot = null;
        double leafsCost = 0;

        slots = new HashMap<Table, TableAccessSlot>();

        for (Operator o : leafs()) {

            if (o.getDatabaseObjects().isEmpty())
                // check if the operator is over a database object (Table or Index); ignore if not
                continue;

            slot = new TableAccessSlot(o);

            if (slots.get(slot.getTable()) != null)
                // we don't allow more than one slot for a table
                throw new SQLException(slot.getTable() + " referenced more than once");

            slots.put(slot.getTable(), slot);

            leafsCost += extractCostOfLeaf(plan, o);
        }

        if (slots.size() == 0)
            throw new SQLException("No slot identified in given plan");

        internalPlanCost = getRootOperator().getAccumulatedCost() - leafsCost;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<Index> getIndexes()
    {
        List<Index> indexes = new ArrayList<Index>();

        for (TableAccessSlot s : slots.values())

            if (s.getIndex() instanceof FullTableScanIndex)
                continue;
            else
                indexes.add(s.getIndex());

        return indexes;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<Table> getTables()
    {
        return new ArrayList<Table>(slots.keySet());
    }

    /**
     * Returns the internal cost of the "template plan".
     *
     * @return
     *      the cost of the internal subplan, i.e. the cost of the original plan minus the cost of
     *      the leafs
     */
    public double getInternalCost()
    {
        return internalPlanCost;
    }

    /**
     * The cost for the plan, considering that the given atomic configuration is used in each slot.
     *
     * @param atomicConfiguration
     *      a configuration where every index corresponds to a different table
     * @return
     *      cost if the given configuration is used to execute the plan. The cost {@link
     *      Double#POSITIVE_INFINITY} if an index isn't compatible with a table slot
     * @throws SQLException
     *      if two or more indexes reference the same table
     */
    public double plug(Collection<Index> atomicConfiguration) throws SQLException
    {
        Set<Table> visited = new HashSet<Table>();
        double c = 0;

        for (Index i : atomicConfiguration) {

            if (visited.contains(i.getTable()))
                throw new SQLException("Not an atomic configuration: " + atomicConfiguration);

            visited.add(i.getTable());

            c += plug(i);
        }

        if (visited.size() != slots.size())
            throw new SQLException(
                "One or more tables missing in atomic configuration.\n" +
                "  Tables in atomic " + getReferencedTables(atomicConfiguration) + "\n" +
                "  Tables in stmt: " + getTables() + "\n" +
                "  For statement:\n" + getStatement() + "\n" +
                "  Plan: \n" + this);

        return c;
    }

    /**
     * The cost for the corresponding {@link TableSlot table slot}, considering that the given index
     * is used to execute the operator.
     *
     * @param index
     *      used to "instantiate" the corresponding slot
     * @return
     *      cost if the given index is used to execute the corresponding {@link TableSlot table
     *      slot}. {@link Double#POSITIVE_INFINITY} if the index isn't compatible with the slot
     * @throws SQLException
     *      if the plan doesn't contain a slot for {@code index.getTable()}
     */
    public double plug(Index index) throws SQLException
    {
        TableAccessSlot slot = slots.get(index.getTable());

        if (slot == null)
            throw new SQLException("Plan doesn't contain a slot for table " + index.getTable());

        if (slot.getIndex().equals(index) || slot.getIndex().equalsContent(index))
            // if we have the same index as when we built the template
            return slot.getCost();

        if (!(slot.getIndex() instanceof FullTableScanIndex) && !slot.isCompatible(index))
            return Double.POSITIVE_INFINITY;

        // we have an index that we haven't seen before, so we need to invoke the optimizer
        SQLStatementPlan plan =
            delegate.explain(
                    buildQueryForUnseenIndex(slot, index),
                    Sets.<Index>newHashSet(index)).getPlan();

        if (plan.leafs().size() != 1)
            throw new SQLException("Plan should have only one leaf");

        Operator o = Iterables.<Operator>get(plan.leafs(), 0);

        if (o.getDatabaseObjects().isEmpty())
            throw new SQLException("The slot should not be empty.");

        if (o.getDatabaseObjects().get(0) instanceof Index)
            return plan.getRootOperator().accumulatedCost;

        // plan uses a full table scan, so it's not compatible
        return Double.POSITIVE_INFINITY;
    }

    /**
     * Returns the slot corresponding to the given table.
     *
     * @param table
     *      the given table
     * @return
     *      a {@code TableAccessSlot} object or {@code null} if the table is not referenced by the
     *      statement
     */
    public TableAccessSlot getSlot(Table table)
    {
        return slots.get(table);
    }

    /**
     * Returns the slots of the template plan.
     *
     * @return
     *      a set of {@code TableAccessSlot} objects
     */
    public Collection<TableAccessSlot> getSlots()
    {
        return slots.values();
    }

    /**
     * Builds a query for an unseen index. An unseen index is one that wasn't part of the
     * interesting order enumeration that was used to build a template plan, thus the cost of using
     * it can't be inferred from the template and has to be estimated through the DBMS' optimizer.
     * <p>
     * This method assumes that the index is compatible with the slot, i.e. that {@code
     * slot.getTable == index.getTable()} is {@code true}.
     *
     * @param slot
     *      slot that corresponds to the index
     * @param index
     *      the index
     * @return
     *      a SQL statement that can be used to retrieve the cost of using the index at the given
     *      slot
     * @throws SQLException
     *      if no columns fetched for the corresponding table
     */
    private static SQLStatement buildQueryForUnseenIndex(TableAccessSlot slot, Index index)
        throws SQLException
    {
        StringBuilder sb = new StringBuilder();

        if (slot.getColumnsFetched().size() == 0)
            throw new SQLException("No columns fetched; something must be wrong");

        // Assume the relation of index is R
        //
        //   SELECT
        //       (slot.getColumnsFetched)
        //   FROM
        //       R
        //   WHERE
        //       (predicates on columns of R)
        //   ORDER BY
        //       (slot.getIndex())

        sb.append("SELECT ");

        for (Column c : slot.getColumnsFetched().columns())
            sb.append(c.getName()).append(", ");

        sb.delete(sb.length() - 2, sb.length() - 1);

        sb.append(" FROM ").append(slot.getTable().getFullyQualifiedName());

        if (slot.getPredicates().size() > 0) {

            sb.append(" WHERE ");

            for (Predicate p : slot.getPredicates())
                sb.append(p.getText()).append(" AND ");

            sb.delete(sb.length() - 5, sb.length() - 1);
        }

        if (!(slot.getIndex() instanceof FullTableScanIndex)) {

            sb.append(" ORDER BY ");

            for (Column c : slot.getIndex().columns())
                sb.append(c.getName())
                    .append(slot.getIndex().isAscending(c) ? " ASC, " : " DESC, ");

            sb.delete(sb.length() - 2, sb.length() - 1);
        }

        return new SQLStatement(sb.toString());
    }

    /**
     * Returns the cost of the given leaf.
     *
     * @param sqlPlan
     *      plan which the leaf is contained in
     * @param leaf
     *      operator at the base of the plan
     * @return
     *      cost of the leaf
     */
    static double extractCostOfLeaf(SQLStatementPlan sqlPlan, Operator leaf)
    {
        if (leaf.getName().equals(Operator.TABLE_SCAN))
            return leaf.getAccumulatedCost();

        Operator parent = sqlPlan.getParent(leaf);

        if (parent == null)
            throw new RuntimeException("Something is wrong, leaf should have a parent");

        if (parent.getName().equals(Operator.FETCH) || parent.getName().equals(Operator.RID_SCAN))
            return parent.getAccumulatedCost();

        return leaf.getAccumulatedCost();
    }
}
