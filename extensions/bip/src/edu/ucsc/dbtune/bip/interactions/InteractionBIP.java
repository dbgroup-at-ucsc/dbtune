package edu.ucsc.dbtune.bip.interactions;

import java.util.*; 
import java.sql.SQLException;

import edu.ucsc.dbtune.bip.util.QueryPlanDesc;
import edu.ucsc.dbtune.inum.InumSpace;
import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.metadata.Schema;
import edu.ucsc.dbtune.metadata.Table;
import edu.ucsc.dbtune.inum.InumStatementPlan;


/**
 * It serves as the entry point to the computation of index interaction
 * 
 * @author tqtrung@ucsc.edu.sc (Quoc Trung Tran)
 *
 */
public class InteractionBIP {		
	
	private ArrayList<ArrayList<Index>> resultInteractIndexes;
	/**
	 * Given a workload and a set of candidate indexes, derive pair of indexes that interact
	 * 
	 * @param workoadName
	 * 		The given workload name
	 * @throws Exception
	 */
	public void run(String workoadName) throws Exception{
		
		ArrayList<String> listQuery = new ArrayList<String>();	
		ArrayList<InumSpace> listInumSpaces = new ArrayList<InumSpace>();		 
		ArrayList<Index> candidateIndexes = new ArrayList<Index>(); 
		
		String query;
		// TODO:  Read query from the workload into @listQuery
		// Derive the @candidateIndexes
		
		/**
		 *  Interact with INUM to obtain query descriptor
		 *  Each template plan including: internal cost, all possible access cost
		 * 	The access cost is computed once?
		 */ 
		for (Iterator<String> iterQuery = listQuery.iterator(); iterQuery.hasNext(); )
		{		
			query = iterQuery.next();
			listInumSpaces.add(populateInumSpace(query));
		}
		
		
		for (Iterator<InumSpace> iterInum = listInumSpaces.iterator(); iterInum.hasNext();)
		{			
			runOneQuery(iterInum.next(), candidateIndexes);			
		}		
		
	}
	
	/**
	 * This method will be integrated into @run() later 
	 * 
	 * @param listInumSpaces
	 * 		The list of INUM spaces, derived from the given workload
	 * @param  candidateIndexes
	 * 		The list of candidate indexes derived from the given workload
	 * 
	 * @throws Exception
	 */
	public void run(List<InumSpace> listInumSpaces, List <Index> candidateIndexes) throws Exception{
		// Each INUM space corresponds to one query
		resultInteractIndexes = new ArrayList<ArrayList <Index> >();
		for (Iterator<InumSpace> iterInum = listInumSpaces.iterator(); iterInum.hasNext();)
		{			
			runOneQuery(iterInum.next(), candidateIndexes);			
		}
		
		System.out.println("In run(), the list of final interacted indexes: ====");	
		for (Iterator<ArrayList<Index>> iter = resultInteractIndexes.iterator(); iter.hasNext(); )
		{	
			System.out.println(" Pair of interacting indexes: ");
			for (Iterator<Index> iterIndex = iter.next().iterator(); iterIndex.hasNext(); )
			{
				System.out.println(" ----- " + iterIndex.next().getName());
			}			
		}
	}
	/**
	 * Given a particular query inum space (corresponding to one query), derive
	 * pair of indexes that interact with each other
	 * 
	 * @param space
	 * 		The given Inum space
	 * 
	 * @param candidateIndexes
	 * 		The given list of candidate indexes
	 * 
	 * @return void
	 * 
	 */
	public void runOneQuery(InumSpace space, List<Index> candidateIndexes)
	{
		// 1. Generate query descriptor
		QueryPlanDesc desc =  new QueryPlanDesc(); 			
		desc.generateQueryPlanDesc(space, candidateIndexes);
		
		// 2. Call cplex
		IIPCPlex cplex = new IIPCPlex();
		ArrayList<ArrayList<Index>> interactIndexes = cplex.run(desc);
		
		for (Iterator<ArrayList<Index>> iter = interactIndexes.iterator(); iter.hasNext(); )
		{
			ArrayList<Index> pairIndexes = iter.next();
			resultInteractIndexes.add(pairIndexes);
			System.out.println(" Pair of interacting indexes: ");
			for (Iterator<Index> iterIndex = pairIndexes.iterator(); iterIndex.hasNext(); )
			{
				System.out.println(" ----- " + iterIndex.next().getName());
			}			
		}
				
	}
	
		
	
	/**
	 * Interact with INUM to get the INUM's search space 
	 * @param query
	 * 		A SQL query
	 * @return
	 * 	    Inum space including template plans
	 */
	public InumSpace populateInumSpace(String query){
		// TODO: interact with INUM to get the INUM space and Schema 
		throw new RuntimeException("NOT IMPLEMENTED YET");
	}
		
}
