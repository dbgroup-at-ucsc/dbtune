package edu.ucsc.dbtune.bip.util;

import java.sql.SQLException;
import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.metadata.Table;


public class IndexFullTableScan extends Index 
{	
	private Table table;
	public static final String FULL_TABLE_SCAN_SUFFIX =  "_full_table_scan";
	public IndexFullTableScan(Table table) throws SQLException
    {
        super(table.getSchema(), table.getSchema().getName() + "_" + table.getName() + FULL_TABLE_SCAN_SUFFIX);
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
