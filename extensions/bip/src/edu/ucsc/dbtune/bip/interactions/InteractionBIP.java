package edu.ucsc.dbtune.bip;

import java.util.*; 
import java.sql.SQLException;

import edu.ucsc.dbtune.SharedFixtures;
import edu.ucsc.dbtune.inum.InumSpace;
import edu.ucsc.dbtune.inum.InMemoryInumSpace;
import edu.ucsc.dbtune.inum.Inum;

import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.inum.OptimalPlan;
import edu.ucsc.dbtune.inum.InumStatementPlan;

/**
 * It serves as the entry point to the computation of index interaction
 * 
 * @author tqtrung@ucsc.edu.sc (Quoc Trung Tran)
 *
 */
public class BIP {
		
	private static ArrayList<String> listQuery;
	private static ArrayList<QueryDescPlan> listQueryDesc;
	private static ArrayList<Index> candidateIndexes;
	private static Inum inum;

	public void run(){
	
	}
	
	public static void main(String[] args) throws Exception{
		CPlex cplex = new CPlex();
		try{
			// TODO: check where is @confiureInum()
			inum = SharedFixtures.configureInum();
		} catch (Exception e){
			
		}
	    inum.start();
		
		String query;
		// TODO:  Read query from the workload into @listQuery
		
		/**
		 *  Interact with INUM to obtain query descriptor
		 *  Each template plan including: internal cost, all possible access cost
		 * 	The access cost is computed once?
		 */ 
		for (Iterator<String> iterQuery = listQuery.iterator(); iterQuery.hasNext(); ){
			query = iterQuery.next();
			listQueryDesc.add(generatePlanDescription(query));
		}
		
		// Optimize (reduce the number of call to INUM)?
		for (Iterator<QueryDescPlan> iterDesc = listQueryDesc.iterator(); iterDesc.hasNext();){
			candidateIndexes = new ArrayList<Index>();
			
			
			// TODO: generate @candidateIndexes
			cplex.run(iterDesc.next(), candidateIndexes);
		}
		
		inum.end();
	}
	
	/**
	 * Interact with INUM to get the INUM's search space 
	 * @param query
	 * 		A SQL query
	 * @return
	 * 	    A query plan descriptor, including information about (internal plan + access costs)
	 */
	public static QueryDescPlan generatePlanDescription(String query){
		QueryDescPlan desc = new QueryDescPlan();
		
		// TODO: communicate with INUM to populate the information 
		// into @space		
		InumSpace space = inum.getInumSpace();
		
		Set<InumStatementPlan> optimalPlans = space.getTemplatePlans();
		
		for (InumStatementPlan plan : optimalPlans){
			
		}
		
		
		
		return desc;
	}
	
	
		
}
