package edu.ucsc.dbtune.bip.util;

import edu.ucsc.dbtune.metadata.Index;

/**
 * This class augments information of an index class, such as
 *    - Type of index: CREATE, DROP, REMAIN in the system
 *    - Estimated size if it is materialized
 *    - Time to materialize
 *    - etc.
 * 
 * @author tqtrung
 *
 */
public class MatIndex 
{
	public static final int INDEX_TYPE_CREATE = 1000;
	public static final int INDEX_TYPE_DROP = 1001;
	public static final int INDEX_TYPE_REMAIN = 1002;
	
	private int posInMatPool;
	private int typeMatIndex;
	private int matWindow;
	private int replicaID;
	private double sizeMatIndex;
	private Index index;
	
	public MatIndex(Index _index, int _ID, int _type)	
	{
	        
		this.index = _index;
		this.posInMatPool = _ID;
		this.typeMatIndex = _type;
	}
	
	
	/**
	 * 
	 * Get / Set methods 
	 * 
	 */
	public void setIndex(Index index)
	{
		this.index = index;
	}
	
	public Index getIndex()
	{
		return index;
	}
	
	public void setId(int Id)
	{
		this.posInMatPool = Id;
	}
	
	public int getId()
	{
		return posInMatPool;
	}
	
	public void setTypeMatIndex(int type)
	{
		this.typeMatIndex = type;
	}
	
	public int getTypeMatIndex()
	{
		return this.typeMatIndex;
	}
	
	public void setMatWindow(int matWindow)
	{
		this.matWindow = matWindow;
	}
	
	public int getMatWindow()
	{
		return this.matWindow;
	}
	
	public void setMatSize(double size)
	{
		this.sizeMatIndex = size;
	}
	
	public double getMatSize()
	{
		return sizeMatIndex;
	}
	
	public void setReplicaID(int _ID)
	{
	    this.replicaID = _ID;
	}
	
	public int getReplicaID()
	{
	    return this.replicaID;
	}


    @Override
    public String toString() {
        return "MatIndex [index=" + index.getFullyQualifiedName() + ", replicaID=" + replicaID + "]";
    }
}
