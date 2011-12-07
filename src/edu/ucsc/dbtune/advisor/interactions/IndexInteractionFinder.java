package edu.ucsc.dbtune.advisor.interactions;


import edu.ucsc.dbtune.metadata.Configuration;
import edu.ucsc.dbtune.metadata.Index;
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
	
	public static class IndexInteraction 
	{
		private Index a;
		private Index b;
		
		public IndexInteraction(Index _a, Index _b) 
		{
			a = _a;
			b = _b;
		}
		
		public Index getFirst()
		{
			return a;
		}
		
		public Index getSecond()
		{
			return b;
		}
	}
}
