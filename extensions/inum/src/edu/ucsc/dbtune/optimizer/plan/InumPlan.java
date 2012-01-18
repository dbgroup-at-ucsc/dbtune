package edu.ucsc.dbtune.optimizer.plan;

import java.sql.SQLException;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.metadata.Table;

/**
 */
public class InumPlan extends SQLStatementPlan
{
    /**
     * The cost of the internal nodes of a plan; the paper refers to it as {@latex.inline 
     * $\\beta_k$}
     */
    private double internalPlanCost;

    /**
     * Convenience object that maps tables to slots. This is done so that the {@link 
     * #getAccessSlots} and {@link #plugIntoSlots} method can run efficiently
     */
    private Map<Table, TableAccessSlot> slots;

    /**
     * Creates a new instance of an Inum plan. 
     */
    public InumPlan(SQLStatementPlan plan) throws SQLException
    {
        super(plan);

        double leafsCost = 0;
        TableAccessSlot slot;

        slots = new HashMap<Table, TableAccessSlot>();

        for (Operator o : leafs()) {

            // check if the operator is over a database object (Table or Index) if not, then we 
            // ignore it
            if (o.getDatabaseObjects().isEmpty())
                continue;

            slot = new TableAccessSlot(o);

            slots.put(slot.getTable(), slot);
            leafsCost += o.getAccumulatedCost();
        }

        internalPlanCost = getRootOperator().getAccumulatedCost() - leafsCost;
    }

    /**
     */
    public double getInternalCost()
    {
        return internalPlanCost;
    }

    /**
     * @return
     *      the cost if the atomic configuration is used in each slot; {@link 
     *      Double#POSITIVE_INFINITY} if some of the indexes in the configuration can't be used in a 
     *      slot.
     */
    public double plug(Set<Index> atomicConfiguration)
    {
        throw new RuntimeException("not yet");
    }

    public Collection<TableAccessSlot> getAccessSlots()
    {
        return slots.values();
    }
    
    /**
     * @return
     *      the cost if the atomic configuration is used in each slot; {@link 
     *      Double#POSITIVE_INFINITY} if some of the indexes in the configuration can't be used in a 
     *      slot.
     */
    public double plug(Index index)
    {
        throw new RuntimeException("not yet");
    }
}
