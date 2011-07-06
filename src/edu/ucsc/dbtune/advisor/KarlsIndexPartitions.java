package edu.ucsc.dbtune.advisor;

import edu.ucsc.dbtune.core.DBIndex;
import edu.ucsc.dbtune.ibg.CandidatePool.Snapshot;
import edu.ucsc.dbtune.util.IndexBitSet;
import java.util.LinkedList;
import java.util.ListIterator;

/**
 * ...
 *
 * @author hsanchez@cs.ucsc.edu (Huascar A. Sanchez)
 */
public class KarlsIndexPartitions<I extends DBIndex> {
	private static final int MAXIMUM_INDEX_COUNT = Integer.MAX_VALUE / 2;
	private final int indexCount;
	private int stateCount;
	private SubsetList subsets;

	public KarlsIndexPartitions(StaticIndexSet<I> indexes) {
		if (indexes.size() > MAXIMUM_INDEX_COUNT)
			throw new IllegalArgumentException("Cannot create partitions for " + indexes.size() + "indexes");
		indexCount = indexes.size();
		stateCount = indexes.size() * 2;
		subsets = new SubsetList();
		for (I index : indexes) {
			subsets.add(new Subset(index));
		}
	}

	public KarlsIndexPartitions(Snapshot<I> snapshot, IndexBitSet[] partitionBitSets) {
		// create subsets
		int indexCount0 = 0;
		int stateCount0 = 0;
		subsets = new SubsetList();
		for (IndexBitSet bs : partitionBitSets) {
			Subset subset = null;
			for (int i = bs.nextSetBit(0); i >= 0; i = bs.nextSetBit(i+1)) {
				I idx = snapshot.findIndexId(i);
				if (idx != null) {
					if (subset != null)
						subset = new Subset(subset, new Subset(idx));
					else
						subset = new Subset(idx);
				}
			}
			if (subset != null) {
				subsets.add(subset);
				indexCount0 += subset.size();
				stateCount0 += subset.stateCount();
			}
		}

		if (indexCount0 > MAXIMUM_INDEX_COUNT)
			throw new IllegalArgumentException("Cannot create partitions for " + indexCount0 + "indexes");

		indexCount = indexCount0;
		stateCount = stateCount0;
	}

	public int indexCount() {
		return indexCount;
	}

	public int subsetCount() {
		return subsets.size();
	}

	public int wfaStateCount() {
		return stateCount;
	}

	public final Subset get(int i) {
		return subsets.get(i);
	}

	public IndexBitSet[] bitSetArray() {
		IndexBitSet[] arr = new IndexBitSet[subsets.size()];
		for (int i = 0; i < subsets.size(); i++) {
			arr[i] = subsets.get(i).bitSet();
		}
		return arr;
	}

	public final void merge(I i1, I i2) {
		int s1 = subsets.whichSubset(i1);
		int s2 = subsets.whichSubset(i2);
		merge(s1, s2);
	}

	public void merge(int s1, int s2) {
		if (s1 == s2)
			return;

		Subset subset1 = subsets.get(s1);
		Subset subset2 = subsets.get(s2);
		Subset newSubset = new Subset(subset1, subset2);

		// calculate & check new state count
		long oldStateCount = stateCount;
		long newStateCount = oldStateCount - subset1.stateCount() - subset2.stateCount() + newSubset.stateCount();
		if (newStateCount > Integer.MAX_VALUE)
			throw new IllegalArgumentException("merging subsets results in too many states");

		subsets.remove(s1);
		subsets.remove(s1 < s2 ? (s2-1) : s2);
		subsets.add(newSubset);
		stateCount = (int) newStateCount;
	}

	public <J extends DBIndex> double
	theoreticalCost(ProfiledQuery<J> qinfo, IndexBitSet state, IndexBitSet scratch) {
		// Let's override the nonsense for now
		return qinfo.planCost(state);

//		double cost = 0;
//		for (int s = 0; s < subsets.size(); s++) {
//			scratch.clear();
//			for (int id : subsets.get(s).indexIds) {
//				if (state.get(id))
//					scratch.set(id);
//			}
//
//			cost += qinfo.planCost(scratch);
//		}
//		scratch.clear();
//		cost -= qinfo.planCost(scratch) * (subsets.size() - 1);
//		return cost;
	}

	@Override
	public boolean equals(Object o1) {
		if (!(o1 instanceof IndexPartitions))
			return false;

		KarlsIndexPartitions<?> other = (KarlsIndexPartitions<?>) o1;
		if (indexCount != other.indexCount || stateCount != other.stateCount)
			return false;

		return subsets.equals(other.subsets);
	}

	public class Subset implements Iterable<I> {
		private final int sumIndexIds;
		private final int minIndexIds;

		private int[] indexIds;

		private java.util.TreeMap<Integer,I> map = new java.util.TreeMap<Integer,I>();

		Subset(I index) {
			map.put(index.internalId(), index);
			indexIds = new int[] { index.internalId() };
			sumIndexIds = minIndexIds = index.internalId();
		}

		Subset(Subset s1, Subset s2) {
			for (I idx : s1)
				map.put(idx.internalId(), idx);
			for (I idx : s2)
				map.put(idx.internalId(), idx);

			indexIds = new int[map.size()];
			{ int i = 0; for (I idx : map.values()) indexIds[i++] = idx.internalId(); }

			sumIndexIds = s1.sumIndexIds + s2.sumIndexIds;
			minIndexIds = Math.min(s1.minIndexIds, s2.minIndexIds);
		}

		public final boolean contains(I index) {
			return map.containsKey(index.internalId());
		}

		public final boolean contains(int id) {
			return map.containsKey(id);
		}

		public final int size() {
			return indexIds.length;
		}

		public final long stateCount() {
			return 1L << size();
		}

		public java.util.Iterator<I> iterator() {
			return map.values().iterator();
		}

		public boolean overlaps(Subset other) {
			for (I x : map.values()) {
				if (other.contains(x))
					return true;
			}
			return false;
		}

		@Override
		public boolean equals(Object o) {
			if (!(o instanceof IndexPartitions.Subset))
				return false;
			Subset other = (Subset) o;
			if (other.size() != size())
				return false;

			// iterate over both subsets in sorted order
			java.util.Iterator<Integer> iter1 = map.keySet().iterator();
			java.util.Iterator<Integer> iter2 = other.map.keySet().iterator();
			while (iter1.hasNext()) {
				if (iter1.next() != iter2.next())
					return false;
			}
			return true;
		}

		public IndexBitSet bitSet() {
			IndexBitSet bs = new IndexBitSet();
			for (I x : this) bs.set(x.internalId());
			return bs;
		}

		public int[] indexIds() {
			return indexIds;
		}

		private final float avgIndexId() {
			return sumIndexIds / (float) size();
		}

		private final int minIndexIds() {
			return minIndexIds;
		}
	}

	private class SubsetList {
		LinkedList<Subset> list;

		SubsetList() {
			list = new LinkedList<Subset>();
		}

		final Subset get(int i) {
			return list.get(i);
		}

		@Override
		public final boolean equals(Object o) {
			if (!(o instanceof KarlsIndexPartitions.SubsetList))
				return false;

			SubsetList other = (SubsetList) o;
			return list.equals(other.list);
		}

		final void add(Subset subset) {
			ListIterator<Subset> iter = list.listIterator();

			while (iter.hasNext()) {
				Subset n = iter.next();
				if (subset.avgIndexId() < n.avgIndexId()
						|| (subset.avgIndexId() == n.avgIndexId() && subset.minIndexIds() < n.minIndexIds())) {
					iter.previous();
					iter.add(subset);
					return;
				}
			}

			// add to the end of the list
			list.add(subset);
		}

		final int whichSubset(I index) {
			for (int i = 0; i < list.size(); i++) {
				if (list.get(i).contains(index))
					return i;
			}
			throw new java.util.NoSuchElementException();
		}

		final void remove(int i) {
			list.remove(i);
		}

		final int size() {
			return list.size();
		}
	}
}