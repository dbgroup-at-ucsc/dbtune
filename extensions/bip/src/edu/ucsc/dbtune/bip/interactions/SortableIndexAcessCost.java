package edu.ucsc.dbtune.bip.interactions;

import edu.ucsc.dbtune.metadata.Index;

public class SortableIndexAcessCost implements Comparable<SortableIndexAcessCost>
{
	private double gamma;
	private Index index;
	
	/**
	 * Constructor
	 */
	SortableIndexAcessCost(double gamma, Index index)
	{
		this.gamma = gamma;
		this.index = index;
	}
	
	
	/**
	 * Get/set methods
	 */
	public void setIndexAccessCost(double gamma)
	{
		this.gamma = gamma;
	}
	public double getIndexAccessCost()
	{
		return this.gamma;
	}
	
	
	public Index getIndex()
	{
		return this.index;
	}
	

	@Override
	public int compareTo(SortableIndexAcessCost o) 
	{		
	    double objCost = o.getIndexAccessCost(); 
		if (gamma < objCost)
			return -1;
		else if (gamma == objCost)
			return 0;
		else 
			return 1;
	}
}
