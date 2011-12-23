package edu.ucsc.dbtune.bip.util;

import java.sql.SQLException;
import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.metadata.Table;

/**
 * The class represents a special index: full table scan index per relation 
 * 
 * @author tqtrung
 *
 */
public class IndexFullTableScan extends Index 
{	
	private Table table;
	public static final String FULL_TABLE_SCAN_SUFFIX =  "full_table_scan";
	
	public IndexFullTableScan(Table table) throws SQLException
    {
        super(table.getSchema(), "");
        this.name = getId() + "_" + table.getName() + "_" + FULL_TABLE_SCAN_SUFFIX;
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
