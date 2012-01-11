package edu.ucsc.dbtune.bip.interactions;

public class SortableIndexAcessCost implements Comparable<SortableIndexAcessCost>
{
	private double gamma;
	private int pos;
	
	/**
	 * Constructor
	 */
	SortableIndexAcessCost(double gamma, int pos)
	{
		this.gamma = gamma;
		this.pos  = pos;		
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
	
	public void setPosition(int pos)
	{
		this.pos = pos;
	}
	
	public int getPosition()
	{
		return this.pos;
	}
	

	@Override
	public int compareTo(SortableIndexAcessCost o) 
	{		
	    double objCost = o.getIndexAccessCost(); 
		if (gamma < objCost){
			return -1;
		} else if (gamma == objCost){
			return 0;
		} else {
			return 1;
		}
	}
}
