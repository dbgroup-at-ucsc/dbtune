package edu.ucsc.dbtune.inum;

import java.util.List;
import java.util.Set;

import edu.ucsc.dbtune.optimizer.plan.Operator;
import edu.ucsc.dbtune.optimizer.plan.SQLStatementPlan;
import edu.ucsc.dbtune.workload.SQLStatement;
import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.metadata.Table;

public class InumStatementPlan extends SQLStatementPlan {

	public InumStatementPlan(SQLStatement sql, Operator root) {
		super(sql, root);
		// TODO Auto-generated constructor stub
	}
	
	/**
	   * Return the access of the given index in the corresponding slot
	   * @param index
	   *    The given index 
	   * @return
	   *    The access cost
	   */
	public double getAccessCost(Index index) {
		throw new RuntimeException("NOT IMPLEMENTED YET");
	}
	
	public double getFullTableScanCost(Table table) {
		throw new RuntimeException("NOT IMPLEMENTED YET");
	}
	
	/**
	   * Return the cost of the internal subplan
	   *  
	   * @return
	   *    The cost of the internal subplan
	   */
	public double getInternalCost() {
		 throw new RuntimeException("NOT IMPLEMENTED YET");
	}
	
	/**
     * Return the list of tables referenced by the statement
     *
     * @return
     *     the list of referenced tables
     */
    public List<Table> getReferencedTables()
    {
        throw new RuntimeException("NOT IMPLEMENTED YET");
    }
    
    /**
     * Return the list of tables in the schema
     *
     * @return
     *     the list of referenced tables
     *     
     * {\bf Note } Need to move this method to appropriate place    
     */
    public List<Table> getSchemaTables()
    {
        throw new RuntimeException("NOT IMPLEMENTED YET");
    }
    
    /**
     * Return the size of the given index
     * @param index
     * 		The given index
     * @return
     * 		Index size
     */
    public double getMaterializedIndexSize(Index index) {
    	throw new RuntimeException("NOT IMPLEMENTED YET");
    }
}
