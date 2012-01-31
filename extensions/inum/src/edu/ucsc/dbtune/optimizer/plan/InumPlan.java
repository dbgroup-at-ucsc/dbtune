package edu.ucsc.dbtune.optimizer.plan;

import java.sql.SQLException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.metadata.Table;
import edu.ucsc.dbtune.optimizer.Optimizer;

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
     * We use this optimizer for doing what-if calls to compute
     * index access cost
     */
    private Optimizer delegate; 
    
    /**
     * Creates a new instance of an INUM plan. 
     *
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

        double leafsCost = 0;
        TableAccessSlot slot;

        slots = new HashMap<Table, TableAccessSlot>();

        for (Operator o : leafs()) {

            // check if the operator is over a database object (Table or Index); ignore if not
            if (o.getDatabaseObjects().isEmpty())
                continue;

            slot = new TableAccessSlot(o);

            // we don't allow self-joins
            if (slots.get(slot.getTable()) != null)
                throw new SQLException(
                    slot.getTable() + " referenced more than once; self-joins not supported yet");

            slots.put(slot.getTable(), slot);
            // TODO: add the cost of RIDSCAN into the cost of IXSCAN (leafsCost)
            leafsCost += o.getAccumulatedCost();
        }

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
    public double plug(Set<Index> atomicConfiguration) throws SQLException
    {
        Set<Table> visited = new HashSet<Table>();
        double c = 0;

        for (Index i : atomicConfiguration) {

            if (visited.contains(i.getTable()))
                throw new SQLException("Not an atomic configuration: " + atomicConfiguration);

            visited.add(i.getTable());

            c += plug(i);
        }

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
        
        if (!slot.isCompatible(index)) 
            return Double.POSITIVE_INFINITY;
        
        
        // if both are full table scan
        if (slot.getIndex().equals(index) || slot.getIndex().equalsContent(index)) 
            return slot.getCost();
        
        // Assume the relation of index is R
        // SELECT (attributes of R that are referenced in the statement)
        // FROM R
        // WHERE (predicates on columns of R that are in index)
        // ORDER BY (slot.getIndex())
        String sql = "";
        Set<Index> atomic = new HashSet<Index>();
        atomic.add(index);
        SQLStatementPlan plan = delegate.explain(sql, atomic).getPlan();
        
        if (plan.leafs().size() > 1) 
            throw new RuntimeException(" The plan should have only one leaf.");
        
        for (Operator o : plan.leafs()) {

            // check if the operator is over a database object (Table or Index); ignore if not
            if (o.getDatabaseObjects().isEmpty())
                throw new RuntimeException(" The slot should not be empty.");

            // TODO: get the cost of RID + IXSCAN
            if (o.getDatabaseObjects().get(0) instanceof Index){
                
                
            } else // Plan uses a full table scan
                return Double.POSITIVE_INFINITY;
        }
        
        return Double.POSITIVE_INFINITY;
        
    }
    
    /**
     * Retrieve the slot corresponding to the given table
     * @param table
     *      The given table
     * @return
     *      An {@code TableAccessSlot} object or NULL if the table
     *      is not referenced by the statement
     */
    public TableAccessSlot getSlot(Table table)
    {
        return slots.get(table);
    }
}
