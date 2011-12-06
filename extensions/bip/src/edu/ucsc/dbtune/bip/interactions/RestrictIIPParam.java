
package edu.ucsc.dbtune.bip;

/**
 * The class contains the input to Restrict IIP problem including
 * 		@element delta
 * 			The threshold to determine the interaction;
 * 			i.e., if doi_q(c,d) >= delta, then c and d interact with each other
 * 		@element totalIndex
 * 			The number of candidate Indexes
 * 		@element desc 
 * 			Query plan descriptor (derived from INUM interface)
 * 			including information about the internal plan and access cost
 * 		@element ic, id
 * 			The relation identifier that contains index c (resp. d)
 * 		@element pos_c, pos_d
 * 			The position of indexes c and d in the corresponding list of indexes S[ic] (resp. S[id])
 * 
 * @author tqtrung@soe.ucsc.edu (Quoc Trung Tran) 
 *
 */
public class RestrictIIPParam {
	public double delta; 
	public int totalIndex;
	public QueryDescPlan desc;	
	
	public int ic, id; 
	public int pos_c, pos_d; 
}
