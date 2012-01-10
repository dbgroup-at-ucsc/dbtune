package edu.ucsc.dbtune.bip.util;

/**
 * The position of indexes in slot including: query statement ID, slot ID, and the position
 * of the index in the slot
 *  
 * @author tqtrung@soe.ucsc.edu
 *
 */
public class IndexInSlot 
{
	private int q, i, a;
	private int fHashCode;
	
	/**
	 * Construct an {@code IndexInSlot} object
	 * 
	 * @param _q
	 *     The query ID
	 * @param _i
	 *     The slot ID 
	 * @param _a
	 *     The position of an index in the {@code _i} slot
	 */
	public IndexInSlot(int _q, int _i, int _a)
	{
		q = _q;
		i = _i;
		a = _a;
		fHashCode = 0;
	}
	
	
	@Override
	public boolean equals(Object obj) 
	{
		if (!(obj instanceof IndexInSlot))
            return false;

        IndexInSlot idx = (IndexInSlot) obj;
        
        if (q != idx.q || i != idx.i || a != idx.a) {
        	return false;
        }
        
        return true;
	}

	@Override
	public int hashCode() 
	{
		if (fHashCode == 0) {
			int result = HashCodeUtil.SEED;
			result = HashCodeUtil.hash(result, q);
			result = HashCodeUtil.hash(result, i);
			result = HashCodeUtil.hash(result, a);
			fHashCode = result;
		}
		
		return fHashCode;
	}

    @Override
    public String toString() {
        return "IndexInSlot [a=" + a + ", i=" + i + ", q=" + q + "]";
    }
}
