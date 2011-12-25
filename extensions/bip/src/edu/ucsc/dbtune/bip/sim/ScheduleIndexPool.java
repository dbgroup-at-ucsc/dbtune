package edu.ucsc.dbtune.bip.sim;

import edu.ucsc.dbtune.bip.util.BIPIndexPool;

/**
 * The class stores all materialized indexes to be scheduled
 * @author tqtrung
 *
 */
public class ScheduleIndexPool extends BIPIndexPool
{
	private int startPosCreateIndex, startPosDropIndex, startPosRemainIndex;

    public int getStartPosCreateIndex() 
    {
        return startPosCreateIndex;
    }

    public void setStartPosCreateIndex(int startPosCreateIndex) 
    {
        this.startPosCreateIndex = startPosCreateIndex;
    }

    public int getStartPosDropIndex() 
    {
        return startPosDropIndex;
    }

    public void setStartPosDropIndex(int startPosDropIndex) 
    {
        this.startPosDropIndex = startPosDropIndex;
    }

    public int getStartPosRemainIndex() 
    {
        return startPosRemainIndex;
    }

    public void setStartPosRemainIndex(int startPosRemainIndex) 
    {
        this.startPosRemainIndex = startPosRemainIndex;
    }	
	
	
}
