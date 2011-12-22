package edu.ucsc.dbtune.bip.util;

import java.sql.SQLException;
import java.util.List;
import edu.ucsc.dbtune.inum.InumSpace;
import edu.ucsc.dbtune.metadata.Schema;
import edu.ucsc.dbtune.metadata.Table;
import edu.ucsc.dbtune.workload.SQLStatement;


/**
 * The class associated with one schema
 *      - Communicate with INUM to populate the space for each statement 
 *      - Generate the list of full table scan indexes associated with each relation in the schema
 * 
 * @author tqtrung
 *
 */
public class BIPAgentPerSchema 
{	
	private Schema schema; 
	private List<IndexFullTableScan> listFullTableScanIndexes;
	
	public BIPAgentPerSchema(Schema _schema) throws SQLException
	{   
		this.schema = _schema;
		
		// create a list of full table scan indexes
		for (Table table : schema.tables()){
		    IndexFullTableScan scanIdx = new IndexFullTableScan(table);
		    this.listFullTableScanIndexes.add(scanIdx);
		}
	}
	
	
	/**
	 * Interact with INUM to get the INUM's search space for the given {@code stmt}
	 * 
	 * @param stmt
	 *     A SQL statement
	 * 
	 * @return
	 * 	    The inum space of the given SQL statement
	 */
	public InumSpace populateInumSpace(SQLStatement stmt)
	{
		// TODO: interact with INUM to get the INUM space 
		throw new RuntimeException("NOT IMPLEMENTED YET");
	}	
	
	/**
	 * Retrieve the list of tables in the schema
	 * 
	 * @return
	 *     List of tables in the schema
	 */
	public List<Table> getListSchemaTables()
	{
	    /*
	    List<Table> listSchemaTables = new ArrayList<Table>();
	    for (Table table : schema.tables()) {
	        listSchemaTables.add(table);
	    }
	    return listSchemaTables;
	    */
	    // TODO: return list of tables in the schema
        throw new RuntimeException("NOT IMPLEMENTED YET");
	    
	}

	/**
     * Retrieve the list of full table scan indexes corresponding to relations in the schema
     * 
     * @return
     *     List of full table scan indexes
     */
	public List<IndexFullTableScan> getListFullTableScanIndexes()
	{
	    // TODO: return list of full table scan indexes
        throw new RuntimeException("NOT IMPLEMENTED YET");
	    //return this.listFullTableScanIndexes;
	}
}

