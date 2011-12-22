package edu.ucsc.dbtune.bip.interactions;

import edu.ucsc.dbtune.bip.util.QueryPlanDesc;

public class IIPQueryPlanDesc extends QueryPlanDesc 
{
	/**
	 * Return the global position of the given index at position a in the slot iSlot (i.e., S[iSlot])
	 * @param iSlot
	 * 		The position of slot S[iSlot]
	 * @param a
	 * 		The position of the given index in S[iSlot]
	 * 
	 * @return 
	 * 		Global index 
	 */
	public int globalIndex(int iSlot, int a)
	{
		int result = 0;
		for (int i = 0; i < iSlot; i++){
			result += S.get(i);
		}
		
		result += a;		
		return result;
	}
}
