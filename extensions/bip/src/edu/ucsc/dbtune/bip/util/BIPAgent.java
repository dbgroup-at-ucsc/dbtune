package edu.ucsc.dbtune.bip.util;

import java.util.List;
import edu.ucsc.dbtune.inum.InumSpace;
import edu.ucsc.dbtune.workload.Workload;

/**
 * The class communicates with INUM to populate Inum space
 * 
 * @author tqtrung
 *
 */
public class BIPAgent 
{	
	private Workload W;	
	public BIPAgent(Workload W)
	{
		this.W = W;
	}
	
	/**
	 * Interact with INUM to get the INUM's search space 
	 * 
	 * @return
	 * 	    List of inum spaces corresponding to the set of given SQL statement
	 */
	public List<InumSpace> populateInumSpace()
	{
		// TODO: interact with INUM to get the INUM space and Schema 
		throw new RuntimeException("NOT IMPLEMENTED YET");
	}	 

}
