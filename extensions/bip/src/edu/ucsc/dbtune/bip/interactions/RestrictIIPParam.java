package edu.ucsc.dbtune.bip.interactions;

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
public class RestrictIIPParam 
{
	private double delta;		
	private int ic, id; 
	private int pos_c, pos_d;
	
	RestrictIIPParam(double delta, int ic, int id, int pos_c, int pos_d) {
		this.delta = delta;
		this.ic = ic;
		this.id = id;
		
		this.pos_c = pos_c;
		this.pos_d = pos_d;
	}
	
	/**
	 * Threshold value to determine index interaction
	 */
	public double getDelta() {
		return delta;
	}
	
	public void setDelta(double delta) {
		this.delta = delta;
	}

	
	/**
	 * Position of relation (or slot) containing @c index
	 */
	public int getPosRelContainC() {
		return ic;
	}
	
	public void setPosRelContainC(int ic) {
		this.ic = ic;
	}
	
	/**
	 * Position of relation (or slot) containing index @d
	 */
	public int getPosRelContainD() {
		return id;
	}
	
	public void setPosRelContainD(int id) {
		this.id = id;
	}
	
	
	/**
	 * Position of index @c in the corresponding slot
	 */
	public int getLocalPosIndexC() {
		return pos_c;
	}
	
	public void setLocalPosIndexC(int pos_c) {
		this.pos_c  = pos_c;
	}
	
	
	/**
	 * Position of index @d in the corresponding slot
	 */
	public int getLocalPosIndexD() {
		return pos_d;
	}
	
	public void setLocalPosIndexD(int pos_d) {
		this.pos_d  = pos_d;
	}
}
