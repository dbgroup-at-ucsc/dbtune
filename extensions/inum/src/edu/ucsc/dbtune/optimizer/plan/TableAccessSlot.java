package edu.ucsc.dbtune.optimizer.plan;

import java.sql.SQLException;

import edu.ucsc.dbtune.inum.FullTableScanIndex;
import edu.ucsc.dbtune.metadata.DatabaseObject;
import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.metadata.Table;

/**
 * Represents a slot in an INUM template plan.
 *
 * @author Ivo Jimenez
 * @see InumPlan
 */
public class TableAccessSlot extends Operator
{
    private Index index;

    /**
     * Analyzes the database objects referenced in the given operator and creates a slot 
     * corresponding to it. If the database object referenced is a table (i.e. a FTS is being done), 
     * the slot will have the  {@link IndexFullTableScan FTS index singleton}.
     *
     * @param leaf
     *      the operator from which the slot is being extracted
     * @throws SQLException
     *      if the given operator references more than one database object; if the database object 
     *      is not of type {@link Table} or {@link Index}.
     * @see IndexFullTableScan
     */
    public TableAccessSlot(Operator leaf) throws SQLException
    {
        super(leaf);

        if (!leaf.getName().equals(TABLE_SCAN) && !leaf.getName().equals(INDEX_SCAN))
            throw new SQLException("Only instantiate " + INDEX_SCAN + " or " + TABLE_SCAN);

        if (leaf.getDatabaseObjects().size() != 1)
            throw new SQLException("Leaf should contain one object");

        if (leaf.getColumnsFetched() == null || leaf.getColumnsFetched().size() == 0)
            throw new SQLException("No columns fetched for leaf");
        
        DatabaseObject object = leaf.getDatabaseObjects().get(0);
        
        if (object instanceof Table)
            index = FullTableScanIndex.getFullTableScanIndexInstance((Table) object);
        else if (object instanceof Index)
            index = (Index) object;
        else
            throw new SQLException("Can't proceed with object type " + object.getClass().getName());

        if (index == null)
            throw new SQLException("Can't determine object type associated to leaf node: " + leaf);

        name = "TABLE.ACCESS.SLOT";
    }

    /**
     * Copy constructor.
     *
     * @param other
     *      other object
     */
    public TableAccessSlot(TableAccessSlot other)
    {
        super(other);

        this.index = other.index;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Operator duplicate()
    {
        return new TableAccessSlot(this);
    }

    /**
     * Compares the given index with the one corresponding to this slot in order to check that the 
     * sorted order for is the same. 
     *
     * @param index
     *      index that is compared against this slot's
     * @return
     *      {@code true} if {@code index} is compatible with this slot's index; {@code false} 
     *      otherwise. For full table scans, {@code true} means that both, the given and this slot's 
     *      index are both instances of {@link FullTableScanIndex} and defined over the same table
     */
    public boolean isCompatible(Index index)
    {
        return this.index.isCoveredBy(index);
    }

    /**
     * Returns the index to which this slot corresponds to.
     *
     * @return
     *      corresponding index
     */
    public Index getIndex()
    {
        return index;
    }

    /**
     * Returns the table to which this slot corresponds to.
     *
     * @return
     *      corresponding table
     */
    public Table getTable()
    {
        return index.getTable();
    }

    /**
     * Whether or not the original plan was using FTS for this slot.
     *
     * @return
     *      whether or not the slot was constructed assuming a FTS on this slot
     */
    public boolean isCreatedFromFullTableScan()
    {
        return getIndex() instanceof FullTableScanIndex;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int hashCode()
    {
        int code = 37 * super.hashCode() + index.hashCode();
        System.out.println("   TableAccessSlot.hashCode: " + code);
        return code;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean equals(Object o)
    {
        if (!super.equals(o))
            return false;

        if (this == o)
            return true;

        if (!(o instanceof TableAccessSlot))
            return false;

        TableAccessSlot op = (TableAccessSlot) o;

        if (index.equalsContent(op.index))
            return true;

        return false;
    }
}
