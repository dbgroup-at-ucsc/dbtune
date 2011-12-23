package edu.ucsc.dbtune.bip.util;


public class IndexInSlot 
{
	private int q, i, a;
	private int fHashCode;
	
	public IndexInSlot(int _q, int _i, int _a)
	{
		q = _q;
		i = _i;
		a = _a;
		fHashCode = 0;
	}
	
	public int getSlotPosition()
	{
		return i;
	}
	
	public int getPosInSlot()
	{
		return a;
	}
	
	public int getQueryId()
	{
		return q;
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
