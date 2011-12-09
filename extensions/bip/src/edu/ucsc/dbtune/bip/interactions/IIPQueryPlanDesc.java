package edu.ucsc.dbtune.bip.interactions;

import edu.ucsc.dbtune.bip.util.QueryPlanDesc;

public class IIPQueryPlanDesc extends QueryPlanDesc 
{
	/**
	 * Return the global position of the given index at position a in the slot i (i.e., S[i])
	 * @param index
	 * 		The position of relation S[index]
	 * @param a
	 * 		The position of the given index in S[index]
	 * 
	 * @return 
	 * 		Global index 
	 */
	public int globalIndex(int index, int a)
	{
		int result = 0, i;
		for (i = 0; i < index; i++){
			result += S.get(i);
		}
		
		result += a;		
		return result;
	}
}
