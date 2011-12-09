package edu.ucsc.dbtune.bip.interactions;


public class SortableIndexAcessCost implements Comparable 
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
	public int compareTo(Object o) 
	{
		if(!(o instanceof SortableIndexAcessCost)){
            throw new ClassCastException("Invalid object");
        }
		
		if (gamma < ((SortableIndexAcessCost)o).getIndexAccessCost()){
			return -1;
		} else if (gamma == ((SortableIndexAcessCost)o).getIndexAccessCost()){
			return 0;
		} else {
			return 1;
		}
	}
}
