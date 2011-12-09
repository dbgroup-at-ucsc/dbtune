package edu.ucsc.dbtune.advisor.interactions;

import edu.ucsc.dbtune.metadata.Index;

public class IndexInteraction 
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
