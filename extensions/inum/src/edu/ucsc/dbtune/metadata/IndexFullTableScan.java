package edu.ucsc.dbtune.metadata;

import java.sql.SQLException;
import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.metadata.Table;

/**
 * @author Trung Tran
 */
public class IndexFullTableScan extends Index 
{   
    private Table table;
    
    public IndexFullTableScan(Table table) throws SQLException
    {
        super(table.getSchema(), "");
        this.name = "full_table_scan";
        this.table = table;
    }
    
    /**
     * Returns the table on which the index is defined.
     *
     * @return
     *     the table that this index refers to.
     */
    @Override
    public Table getTable()
    {
        return table;
    }
}
