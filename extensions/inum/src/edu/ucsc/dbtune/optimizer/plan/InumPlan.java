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
import edu.ucsc.dbtune.metadata.DatabaseObject;
import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.metadata.Table;
import edu.ucsc.dbtune.optimizer.Optimizer;
import edu.ucsc.dbtune.workload.SQLStatement;

import static edu.ucsc.dbtune.optimizer.plan.Operator.INDEX_SCAN;
import static edu.ucsc.dbtune.optimizer.plan.Operator.TABLE_SCAN;
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
     * Convenience object that maps tables to slots. This is done so that the {@link
     * #getAccessSlots} and {@link #plugIntoSlots} method can run efficiently
     */
    protected Map<Table, TableAccessSlot> slots;

    /**
     * The cost of the internal nodes of a plan; the paper refers to it as {@latex.inline
     * $\\beta_k$}.
     */
    private double internalPlanCost;

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
     *      if the plan is referencing a table more than once or if one of the leafs doesn't refer 
     *      any database object.
     */
    public InumPlan(Optimizer delegate, SQLStatementPlan plan) throws SQLException
    {
        super(plan);

        this.delegate = delegate;

        TableAccessSlot slot = null;
        double leafsCost = 0;

        slots = new HashMap<Table, TableAccessSlot>();

        for (Operator leaf : leafs()) {

            if (leaf.getDatabaseObjects().size() != 1)
                throw new SQLException("Leaf " + leaf + " doesn't refer to any object");

            leafsCost += extractCostOfLeaf(this, leaf);

            slot = new TableAccessSlot(leaf);

            if (slots.get(slot.getTable()) != null)
                // we don't allow more than one slot for a table
                throw new SQLException(slot.getTable() + " referenced more than once");

            replaceLeafBySlot(this, leaf, slot);

            slots.put(slot.getTable(), slot);
        }

        if (plan.leafs().size() != slots.size())
            throw new SQLException("One or more leafs haven't been assigned with a slot");

        internalPlanCost = getRootOperator().getAccumulatedCost() - leafsCost;
    }

    /**
     * copy constructor.
     *
     * @param other
     *      plan being copied
     */
    InumPlan(InumPlan other)
    {
        super(other);

        slots = other.slots;
        internalPlanCost = other.internalPlanCost;
        delegate = other.delegate;
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
     * The cost for the plan, considering that the given atomic configuration is used in each slot, 
     * as well as the internal cost.
     *
     * @param atomicConfiguration
     *      a configuration where every index corresponds to a different table
     * @return
     *      {@code true} if all the indexes in the given configuration are compatible; {@code false} 
     *      otherwise.
     * @throws SQLException
     *      if two or more indexes reference the same table
     */
    public double plug(Collection<Index> atomicConfiguration) throws SQLException
    {
        SQLStatementPlan plan = instantiate(atomicConfiguration);

        if (plan == null)
            return Double.POSITIVE_INFINITY;

        return plan.getRootOperator().getAccumulatedCost();
    }

    /**
     * The cost for the corresponding {@link TableAccessSlot table slot}, considering that the given 
     * index is used to execute the operator.
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
        Operator o = instantiate(index);

        if (o == null)
            return Double.POSITIVE_INFINITY;

        return o.getAccumulatedCost();
    }

    /**
     * Instantiates a slot by plugging the given index in the corresponding slot.
     *
     * @param index
     *      index being plugged
     * @throws SQLException
     *      if the plan doesn't contain a slot for {@code index.getTable()}
     * @return
     *      a new operator that results from plugging the index in the corresponding slot
     */
    public Operator instantiate(Index index) throws SQLException
    {
        TableAccessSlot slot = slots.get(index.getTable());

        if (slot == null)
            throw new SQLException("Plan doesn't contain a slot for table " + index.getTable());

        if (slot.getIndex().equals(index) || slot.getIndex().equalsContent(index))
            // if we have the same index as when we built the template
            return makeOperator(slot);

        if (!slot.isFullTableScan() && !slot.isCompatible(index))
            return null;

        return instantiateOperatorForUnseenIndex(buildQueryForUnseenIndex(slot), index);
    }

    /**
     * @param sql
     *      statement to be explained in order to obtain an {@link Operator#INDEX_SCAN} operator
     * @param index
     *      the index being sent as the hypothetical configuration
     * @return
     *      an {@link Operator#INDEX_SCAN} operator
     * @throws SQLException
     *      if the statement can't be explained
     */
    public Operator instantiateOperatorForUnseenIndex(SQLStatement sql, Index index)
        throws SQLException
    {
        delegate.setFTSDisabled(true);

        SQLStatementPlan plan = delegate.explain(sql, Sets.<Index>newHashSet(index)).getPlan();

        delegate.setFTSDisabled(false);

        if (plan.leafs().size() != 1)
            throw new SQLException("Plan should have only one leaf");

        Operator o = Iterables.<Operator>get(plan.leafs(), 0);

        if (o.getDatabaseObjects().size() != 1)
            throw new SQLException("The leaf should have one database object.");

        if (!o.getName().equals(INDEX_SCAN))
            throw new SQLException("The leaf should be an " + INDEX_SCAN);

        o.setAccumulatedCost(plan.getRootOperator().getAccumulatedCost());

        // create a new Operator to avoid memory leaks (by keeping the whole plan hanging)
        return new Operator(o);
    }

    /**
     * instantiates an operator out of a slot.
     *
     * @param slot
     *      slot
     * @return
     *      a {@link Operator#TABLE_SCAN} operator if the slot is using a {@link 
     *      FullTableScanIndex}; a {@link Operator#INDEX_SCAN} if the slot uses an {@link Index}.
     */
    private static Operator makeOperator(TableAccessSlot slot)
    {
        DatabaseObject dbo;
        String name;

        if (slot.getIndex() instanceof FullTableScanIndex) {
            name = TABLE_SCAN;
            dbo = slot.getTable();
        } else {
            name = INDEX_SCAN;
            dbo = slot.getIndex();
        }

        Operator op = new Operator(name, slot.getAccumulatedCost(), slot.getCardinality());

        op.add(dbo);
        op.addColumnsFetched(slot.getColumnsFetched());
        op.add(slot.getPredicates());

        return op;
    }

    /**
     * Instantiates a plan by plugging each given index in the corresponding slot.
     *
     * @param atomicConfiguration
     *      configuration used to instantiate the template
     * @throws SQLException
     *      if the plan doesn't contain a slot for one of the tables
     * @return
     *      a new plan that results from plugging the index in the corresponding slot; {@code null} 
     *      if the configuration is not compatible with the template
     */
    public SQLStatementPlan instantiate(Collection<Index> atomicConfiguration)
        throws SQLException
    {
        Set<Table> visited = new HashSet<Table>();
        List<Operator> operators = new ArrayList<Operator>();
        Operator o;

        for (Index i : atomicConfiguration) {

            if (visited.contains(i.getTable()))
                throw new SQLException("Not an atomic configuration: " + atomicConfiguration);

            visited.add(i.getTable());

            o = instantiate(i);

            if (o == null)
                // index i is not compatible to its corresponding slot
                return null;

            operators.add(o);
        }

        if (visited.size() != slots.size())
            throw new SQLException(
                "One or more tables missing in atomic configuration.\n" +
                "  Tables in atomic " + getReferencedTables(atomicConfiguration) + "\n" +
                "  Tables in stmt: " + getTables() + "\n" +
                "  For statement:\n" + getStatement() + "\n" +
                "  Plan: \n" + this);

        return instantiatePlan(this, operators);
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
     * @return
     *      a SQL statement that can be used to retrieve the cost of using the index at the given
     *      slot
     * @throws SQLException
     *      if no columns fetched for the corresponding table
     */
    static SQLStatement buildQueryForUnseenIndex(TableAccessSlot slot)
        throws SQLException
    {
        StringBuilder sb = new StringBuilder();

        if (slot.getColumnsFetched().size() == 0)
            throw new SQLException("No columns fetched; something must be wrong");

        // We build a query like:
        //
        //   SELECT
        //       slot.getColumnsFetched
        //   FROM
        //       slot.getTable
        //   WHERE
        //       slot.getPredicates
        //   ORDER BY
        //       slot.getIndex

        sb.append("SELECT ");

        for (Column c : slot.getColumnsFetched().columns())
            sb.append(c.getName()).append(", ");

        sb.delete(sb.length() - 2, sb.length() - 1);

        sb.append(" FROM ").append(slot.getTable().getFullyQualifiedName());

        if (slot.getPredicates().size() > 0) {

            sb.append(" WHERE ");

            for (Predicate p : slot.getPredicates())
                sb.append(p.getText().replaceAll("'.*'", "\\?")).append(" AND ");

            sb.delete(sb.length() - 5, sb.length() - 1);
        }

        if (slot.getIndex().size() > 0) {

            sb.append(" ORDER BY ");

            for (Column c : slot.getIndex().columns())
                sb.append(c.getName())
                    .append(slot.getIndex().isAscending(c) ? " ASC, " : " DESC, ");

            sb.delete(sb.length() - 2, sb.length() - 1);
        }

        return new SQLStatement(sb.toString());
    }

    /**
     * Returns the cost of the given leaf, taking into account that if it's an {@link 
     * Operator#INDEX_SCAN}, a {@link Operator#FETCH} operator is.
     *
     * @param sqlPlan
     *      plan which the leaf is contained in
     * @param leaf
     *      operator at the base of the plan
     * @return
     *      cost of the leaf
     * @throws SQLException
     *      if {@link Operator#FETCH} is found but it has no parent; if {@link Operator#FETCH} has 
     *      siblings; if an error occurs while pulling down the set of columns fetched from {@link
     *      Operator#FETCH} down to the leaf.
     */
    static double extractCostOfLeaf(SQLStatementPlan sqlPlan, Operator leaf) throws SQLException
    {
        if (leaf.getName().equals(Operator.TABLE_SCAN))
            return leaf.getAccumulatedCost();

        Operator fetch = null;
        Operator parent = sqlPlan.getParent(leaf);
        Index index = (Index) leaf.getDatabaseObjects().get(0);

        while (parent != null) {

            if (parent.getName().equals(Operator.FETCH)) {
                fetch = parent;
                break;
            } else {
                parent = sqlPlan.getParent(parent);
            }
        }
        
        if (fetch == null || !fetch.getDatabaseObjects().get(0).equals(index.getTable()))
            // no FETCH found, or is referring to table distinct from the one the index refers to
            return leaf.getAccumulatedCost();

        if (sqlPlan.getChildren(fetch).size() > 1)
            throw new SQLException(Operator.FETCH + " shouldn't have siblings in\n" + sqlPlan);

        Operator fetchParent = sqlPlan.getParent(fetch);

        if (fetchParent == null)
            throw new SQLException(Operator.FETCH + " should have a dad in\n" + sqlPlan);

        sqlPlan.remove(leaf);

        leaf.setAccumulatedCost(fetch.getAccumulatedCost());
        leaf.add(fetch.getPredicates());
        
        // we also have to pull the columns fetched down to the slot
        for (Column c : parent.getColumnsFetched().columns())
            if (!leaf.getColumnsFetched().contains(c))
                leaf.getColumnsFetched().add(c, parent.getColumnsFetched().isAscending(c));

        sqlPlan.remove(fetch);
        sqlPlan.setChild(fetchParent, leaf);

        return leaf.getAccumulatedCost();
    }

    /**
     * Replaces the leafs in the plan by slots.
     *
     * @param sqlPlan
     *      plan from which the given leaf will be removed and replaced by the slot
     * @param leaf
     *      leaf to be removed
     * @param slot
     *      slot to be inserted instead of the leaf
     * @throws SQLException
     *      if the leaf turns out to be the root operator
     */
    private static void replaceLeafBySlot(
            SQLStatementPlan sqlPlan, Operator leaf, TableAccessSlot slot)
        throws SQLException
    {
        Operator parent = sqlPlan.getParent(leaf);

        if (parent == null)
            throw new SQLException("Leaf " + leaf + " shouldn't be the root operator");

        sqlPlan.remove(leaf);
        sqlPlan.setChild(parent, slot);
    }

    /**
     * Replaces the slots at the leafs by the actual operators used in the instantiated plan.
     *
     * @param templatePlan
     *      template plan being instantiated
     * @param instantiatedOperators
     *      a list containing operators obtained from ({@link #instantiate(Index)})
     * @return
     *      an instance of a plan based on the given template, where each leaf is an {@link 
     *      Operator#TABLE_SCAN} or a {@link Operator#INDEX_SCAN}
     */
    private static SQLStatementPlan instantiatePlan(
            InumPlan templatePlan,
            List<Operator> instantiatedOperators)
    {
        SQLStatementPlan plan = new SQLStatementPlan(templatePlan);
        Operator parent;
        TableAccessSlot slot;
        double cost = 0;

        for (Operator newLeaf : instantiatedOperators) {

            DatabaseObject dbo = newLeaf.getDatabaseObjects().get(0);

            if (dbo instanceof Table || dbo instanceof FullTableScanIndex)
                slot = templatePlan.getSlot((Table) dbo);
            else
                slot = templatePlan.getSlot(((Index) dbo).getTable());

            parent = templatePlan.getParent(slot);

            plan.remove(slot);

            plan.setChild(parent, newLeaf);

            cost += newLeaf.getAccumulatedCost();
        }

        cost += templatePlan.getInternalCost();

        plan.assignCost(plan.getRootElement(), cost);

        return plan;
    }
}
