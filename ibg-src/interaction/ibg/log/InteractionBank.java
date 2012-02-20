package interaction.ibg.log;

import java.io.Serializable;

import interaction.db.DB2IndexSet;
import interaction.util.BitSet;
//import interaction.util.Debug;
import interaction.util.UnionFind;

public class InteractionBank implements Serializable {
	private static final long serialVersionUID = -6233345637898732694L;
	
	public final int indexCount;
	private double[][] lowerBounds; 
	
	protected InteractionBank(DB2IndexSet cands0) {
		indexCount = cands0.maxInternalID() + 1;
		lowerBounds = new double[indexCount][];
		for (int i = 0; i < indexCount; i++) 
			lowerBounds[i] = new double[i];
	}

	public final void clear() {
		for (double[] a : lowerBounds) 
			java.util.Arrays.fill(a, 0.0);
	}
	
	public final void print() {
		
		/* print whole contents */
		System.out.println("--- interactions");
		for (int i = 0; i < indexCount; i++) {
			for (int j = 0; j < i; j++) {
				if (lowerBounds[i][j] > 0.0)
					System.out.printf("%d\t%d\t%8f\n", i, j, lowerBounds[i][j]);
			}
		}
		System.out.println("---");
		
		/* old implementation */
		
		// make a separate UF structure from the instance var
		// so we can get the "possible" rather than the "certain" interactions
//		UnionFind uf = new UnionFind();
//		
//		for (int i = used.nextSetBit(0); i >= 0; i = used.nextSetBit(i+1))
//			uf.makeSet(posToInternalID[i]);
//		
//		System.out.println("Pairwise interactions: ");
//		for (int pos1 = 0; pos1 < indexCount; pos1++) {
//			if (!used.get(pos1)) continue;
//			int id1 = posToInternalID[pos1];
//			System.out.println(id1 + ": ");
//			for (int pos2 = 0; pos2 < pos1; pos2++) {
//				if (!used.get(pos2)) continue;
//				int id2 = posToInternalID[pos2];
//				if (pairwiseInteractionExists(id1, id2) != Interaction.DNE) {
//					uf.union(id1, id2);
//					System.out.println("\t" + id2 + " = " + upperBounds[pos1][pos2]);
//				}
//			}
//		}
//		System.out.println();
//		System.out.println("Stable partitioning: ");
//		uf.print();
	}
	
	/*
	 * Assign interaction with an exact value
	 */
	final void assignInteraction(int id1, int id2, double newValue) {
		assert (newValue >= 0);
		assert (id1 != id2);

		if (id1 < id2) {
			int t = id1;
			id1 = id2;
			id2 = t;
		}
		
		lowerBounds[id1][id2] = Math.max(newValue, lowerBounds[id1][id2]);
	}
	
	final double interactionLevel(int id1, int id2) {
		assert (id1 != id2);	
		if (id1 < id2) 
			return lowerBounds[id2][id1];
		else
			return lowerBounds[id1][id2];
	}
	
//	public final boolean pairwiseInteractionExists(int id1, int id2) {		
//		// the algorithm does not record interactions of an index with itself
//		// caller must not pass in these arguments
//		// if we didn't check, there would be ArrayOutOfBoundsException, but this error message is more informative
//		if (id1 == id2)
//			throw new Error("may not check for interaction between an index and itself");
//		
//		double lower = (id1 < id2) ? lowerBounds[id1][id2] : lowerBounds[id2][id1];
//		return lower >= interactionThreshold;
//	}
//	
//	public final boolean transitiveInteractionExists(int id1, int id2) {
//		return uf.find(id1) == uf.find(id2);
//	}

	public final BitSet[] stablePartitioning(double threshold) {
		UnionFind uf = new UnionFind(indexCount);
		for (int a = 0; a < indexCount; a++) 
			for (int b = 0; b < a; b++) 
				if (lowerBounds[a][b] >= threshold)
					uf.union(a,b);
		return uf.sets();
	}
	
	public void printStablePartitioning(BitSet used, double threshold) {
		// make a separate UF structure from the instance var
		// so we can get the "possible" rather than the "certain" interactions
		UnionFind uf = new UnionFind(indexCount);
		
		System.out.println("Pairwise interactions: ");
		for (int id1 = 0; id1 < indexCount; id1++) {
			if (!used.get(id1)) continue;
			System.out.println(id1 + ": ");
			for (int id2 = 0; id2 < id1; id2++) {
				if (!used.get(id2)) continue;
				if (interactionLevel(id1, id2) > threshold) {
					uf.union(id1, id2);
					System.out.println("\t" + id2 + " = " + lowerBounds[id1][id2]);
				}
			}
		}
		System.out.println();
		System.out.println("Stable partitioning: ");
		uf.print(used);
	}
}
