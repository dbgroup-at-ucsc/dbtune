package edu.ucsc.dbtune.bip.interactions;

import edu.ucsc.dbtune.metadata.Index;

/**
 * The class contains the input to Restrict IIP problem including
 * 		@element delta
 * 			The threshold to determine the interaction;
 * 			i.e., if doi_q(c,d) >= delta, then c and d interact with each other
 * 		@element totalIndex
 * 			The number of candidate Indexes
 * 		@element ic, id
 * 			The relation identifier that contains index c (resp. d)
 * 		@element pos_c, pos_d
 * 			The position of indexes c and d in the corresponding list of indexes S[ic] (resp. S[id])
 * 
 * @author tqtrung@soe.ucsc.edu (Quoc Trung Tran) 
 *
 */
public class RestrictIIPParam 
{
	private double delta;		
	private int ic, id; 
	private Index indexc, indexd;
	
	RestrictIIPParam(double delta, int ic, int id, Index indexc, Index indexd)
	{
		this.delta = delta;
		this.ic = ic;
		this.id = id;
		
		this.indexc = indexc;
		this.indexd = indexd;
	}
	
	/**
	 * Threshold value to determine index interaction
	 */
	public double getDelta() 
	{
		return delta;
	}
	
	
	/**
	 * Position of the relation (or slot) containing the index {@code c}
	 */
	public int getPosRelContainC() 
	{
		return ic;
	}
		
	/**
	 * Position of relation (or slot) containing the index {@code d}
	 */
	public int getPosRelContainD() 
	{
		return id;
	}
	
	
	/**
	 * The first index
	 */
	public Index getIndexC() 
	{
		return indexc;
	}
		
	
	/**
	 * The second index
	 */
	public Index getIndexD() 
	{
		return indexd;
	}
}
