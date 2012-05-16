package interaction.util;

public class BitSet extends java.util.BitSet {
	private static final long serialVersionUID = 1L;
	
	private static java.util.BitSet t = new java.util.BitSet();
	
	public BitSet() {
		super();
	}
	
	@Override
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
	
	// probably better in some cases, worse in others
//	public final boolean subsetOf(BitSet b) {
//		for (int i = this.nextSetBit(0); i >= 0; i = this.nextSetBit(i+1))
//			if (!b.get(i))
//				return false;
//		return true;
//	}
}
