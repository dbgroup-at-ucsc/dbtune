package edu.ucsc.dbtune.bip.util;


public class QueryDescPlan {
	public int Kq; // number of template plans
	public int n; // number of relations used in the query
	public int numIndex; // number of candidate indexes
	public int[] S; // size of each slot
		
	public double[] beta; // internal plan cost
	public double[][][] gamma; 
	// access cost gamma[k][i][a]
	// sort in the increasing order of their value
	

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
	public int globalIndex(int index, int a){
		int result = 0, i;
		for (i = 0; i < index; i++){
			result += S[i];
		}
		
		result += a;		
		return result;
	}
}
