package edu.ucsc.dbtune.bip.sim;

/**
 * The class records the starting and ending position of: the created indexes, then the dropped indexes,
 * and finally the indexes remain in the system (NOT created/dropped)
 * 
 * @author tqtrung@soe.ucsc.edu
 *
 */
public class SchedulePoolLocator 
{
	private int startPosCreateIndex, startPosDropIndex, startPosRemainIndex;

	public SchedulePoolLocator()
	{
	    
	}
	/**
	 * Retrieve the position of the first index of type CREATE in the pool   
	 * @return
	 *     The position in the pool 
	 */
    public int getStartPosCreateIndex() 
    {
        return startPosCreateIndex;
    }

    /**
     * Set the position of the first index of type CREATE in the pool   
     * 
     * {\bf Note}: The value of {@code  startPosCreateIndex} is usually 0
     */
    public void setStartPosCreateIndex(int startPosCreateIndex) 
    {
        this.startPosCreateIndex = startPosCreateIndex;
    }

    /**
     * Retrieve the position of the first index of type DROP in the pool   
     * @return
     *     The position in the pool 
     */
    public int getStartPosDropIndex() 
    {
        return startPosDropIndex;
    }

    /**
     * Set the position of the first index of type DROP in the pool   
     * 
     * {\bf Note}: The value of {@code startPosDropIndex} is usually 
     * equal to the total number of indexes of type CREATE stored in the pool
     */
    public void setStartPosDropIndex(int startPosDropIndex) 
    {
        this.startPosDropIndex = startPosDropIndex;
    }

    /**
     * Retrieve the position of the first index of type REMAIN in the pool   
     * @return
     *     The position in the pool 
     */
    public int getStartPosRemainIndex() 
    {
        return startPosRemainIndex;
    }

    /**
     * Set the position of the first index of type REMAIN in the pool   
     * 
     * {\bf Note}: The value of {@code startPosRemainIndex} is usually 
     * equal to the total number of indexes of type CREATE and DROP stored in the pool
     */
    public void setStartPosRemainIndex(int startPosRemainIndex) 
    {
        this.startPosRemainIndex = startPosRemainIndex;
    }
}
