package edu.ucsc.dbtune.bip.interactions;

import java.sql.SQLException;
import java.util.*;

import edu.ucsc.dbtune.advisor.interactions.IndexInteraction;
import edu.ucsc.dbtune.inum.InumSpace;
import edu.ucsc.dbtune.metadata.Configuration;
import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.workload.SQLStatement;
import edu.ucsc.dbtune.workload.Workload;


/**
 * It serves as the entry point to the computation of index interaction
 * 
 * @author tqtrung@ucsc.edu.sc (Quoc Trung Tran)
 *
 */
public class InteractionBIP 
{	
	/**
	 * Find all pairs of indexes from the given configuration, {@code C}, that 
	 * interact with each other
	 *  
	 * @param W
	 * 		The given workload, consisting of select- and update- statement
	 * @param C
	 * 		The given set of candidate indexes
	 * @param delta
	 * 		The threshold to determine the interaction
	 * 
	 * @return
	 * 		List of pairs of interacting indexes
	 * @throws SQLException 
	 */
	//public List<IndexInteraction> getInteractionIndexes(Workload W, Configuration C, double delta) throws SQLException
	public List<IndexInteraction> getInteractionIndexes(List<InumSpace> listInum, Configuration C, double delta) throws SQLException
	{	
		List<IndexInteraction> resultIndexInteraction = new ArrayList<IndexInteraction>();				 
		List<Index> candidateIndexes = C.toList(); 
		
		/*
		for (Iterator<SQLStatement> iter = W.iterator(); iter.hasNext(); ){
			InumSpace inum = populateInumSpace(iter.next());			
			IIPQueryPlanDesc desc =  new IIPQueryPlanDesc(); 		
			// Populate the information from {@code inum} into a global structure
			// in {@code QueryPlanDesc} object
			desc.generateQueryPlanDesc(inum, candidateIndexes);
			
			IIPCPlex cplex = new IIPCPlex();			
			List<IndexInteraction> listInteraction = cplex.findInteractions(desc, delta);
			for (IndexInteraction pair : listInteraction){
				resultIndexInteraction.add(pair);
			}	
		}
		*/
		
		for (InumSpace inum : listInum){
			IIPQueryPlanDesc desc =  new IIPQueryPlanDesc(); 		
			// Populate the information from {@code inum} into a global structure
			// in {@code QueryPlanDesc} object
			desc.generateQueryPlanDesc(inum, candidateIndexes);
			
			IIPCPlex cplex = new IIPCPlex();			
			List<IndexInteraction> listInteraction = cplex.findInteractions(desc, delta);
			for (IndexInteraction pair : listInteraction){
				resultIndexInteraction.add(pair);
			}
		}
		
		return resultIndexInteraction;
	}
	
	
	/**
	 * Interact with INUM to get the INUM's search space 
	 * @param query
	 * 		A SQL query
	 * @return
	 * 	    Inum space including all template plans
	 */
	public InumSpace populateInumSpace(SQLStatement stmt)
	{
		// TODO: interact with INUM to get the INUM space and Schema 
		throw new RuntimeException("NOT IMPLEMENTED YET");
	}	
}
 