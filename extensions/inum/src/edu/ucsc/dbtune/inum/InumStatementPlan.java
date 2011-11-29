package edu.ucsc.dbtune.inum;

import java.util.Set;

import edu.ucsc.dbtune.optimizer.plan.Operator;
import edu.ucsc.dbtune.optimizer.plan.SQLStatementPlan;
import edu.ucsc.dbtune.workload.SQLStatement;
import edu.ucsc.dbtune.metadata.Index;

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
	
	/**
	   * Return the cost of the internal subplan
	   *  
	   * @return
	   *    The cost of the internal subplan
	   */
	public double getInternalCost() {
		 throw new RuntimeException("NOT IMPLEMENTED YET");
	}
}
