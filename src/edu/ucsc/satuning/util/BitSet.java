package edu.ucsc.satuning.util;

public class BitSet extends java.util.BitSet {
	private static final long serialVersionUID = 1L;
	
	private static final java.util.BitSet t = new java.util.BitSet();
	
	public BitSet() {
		super();
	}
	
	public BitSet clone() {
		return (BitSet) super.clone();
	}
	
	public final void set(BitSet other) {
		clear();
		or(other);
	}
	
	// probably better in average case
	public final boolean subsetOf(BitSet b) {
		synchronized (t) {
			t.clear();
			t.or(this);
			t.and(b);
			return (t.equals(this));
		}
	}
    
}
