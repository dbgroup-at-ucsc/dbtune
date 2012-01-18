package edu.ucsc.dbtune.optimizer.plan;

import java.sql.SQLException;

import edu.ucsc.dbtune.metadata.DatabaseObject;
import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.metadata.IndexFullTableScan;
import edu.ucsc.dbtune.metadata.Table;

/**
 */
public class TableAccessSlot extends Operator
{
    private Table table;
    private Index index;

    /**
     */
    public TableAccessSlot(Operator operator) throws SQLException
    {
        if (operator.getDatabaseObjects().size() != 1)
            throw new SQLException("Only accepting one DB object; operator: " + operator);

        DatabaseObject object = operator.getDatabaseObjects().get(0);

        if (object instanceof Table) {
            table = (Table) object;
            index = new IndexFullTableScan(table);
        } else if (object instanceof Index) {
            index = (Index) object;
            table = index.getTable();
        } else {
            throw new SQLException("Can't proceed with object type " + object.getClass().getName());
        }
    }

    public double getCost(Index index)
    {
        //  compare indexes in order to check that the sorted order for the index is the same as the 
        //  one in the local index
        throw new RuntimeException("Not yet");
    }

    public Table getTable()
    {
        return table;
    }
}
