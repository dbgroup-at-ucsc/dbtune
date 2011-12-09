package edu.ucsc.dbtune.advisor.interactions;


import edu.ucsc.dbtune.metadata.Configuration;
import edu.ucsc.dbtune.workload.Workload;

import java.util.List;


public interface IndexInteractionFinder 
{
	/**
	 * 
	 * @param W
	 * @param C
	 * @param delta
	 * @return
	 */
	public List<IndexInteraction> getInteractingIndexes(Workload W, Configuration C, double delta);	
}
