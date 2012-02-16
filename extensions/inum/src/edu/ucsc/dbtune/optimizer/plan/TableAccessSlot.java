package edu.ucsc.dbtune.optimizer.plan;

import java.sql.SQLException;
import java.util.Hashtable;

import edu.ucsc.dbtune.inum.FullTableScanIndex;
import edu.ucsc.dbtune.metadata.Column;
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
     * cache the cost of plug index into this slot
     */
    public Hashtable<Index,Double> costCache=new Hashtable<Index, Double>();

    /**
     * Analyzes the database objects referenced in the given operator and creates a slot 
     * corresponding to it. If the database object referenced is a table (i.e. a FTS is being done), 
     * the slot will have the  {@link IndexFullTableScan FTS index singleton}.
     *
     * @param leaf
     *      the operator from which the slot is being extracted
     * @param parent
     *      parent of the given leaf
     * @throws SQLException
     *      if the given operator references more than one database object; if the database object 
     *      is not of type {@link Table} or {@link Index}.
     * @see IndexFullTableScan
     */
    public TableAccessSlot(Operator leaf, Operator parent) throws SQLException
    {
        super(leaf);

        if (leaf.getDatabaseObjects().size() != 1)
            throw new SQLException("Leaf should contain only one object");

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
            throw new SQLException("Can't determine object associated to leaf node: " + leaf);

        // checkForFETCHOperatorAndPullDownFetchedColumns();
        if (parent.getName().equals(Operator.FETCH))
            // if parent is a FETCH we have to pull the columns fetched down to the slot
            for (Column c : parent.getColumnsFetched().columns())
                getColumnsFetched().add(c, parent.getColumnsFetched().isAscending(c));

        super.name = "TABLE.ACCESS.SLOT";
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
    public boolean isFullTableScan()
    {
        return getIndex() instanceof FullTableScanIndex;
    }
}
