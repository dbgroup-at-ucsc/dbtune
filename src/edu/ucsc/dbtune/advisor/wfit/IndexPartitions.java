package edu.ucsc.dbtune.advisor.wfit;

import java.util.LinkedList;
import java.util.ListIterator;

import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.advisor.wfit.CandidatePool.Snapshot;
import edu.ucsc.dbtune.advisor.wfit.BitSet;

public class IndexPartitions {
	private static final int MAXIMUM_INDEX_COUNT = Integer.MAX_VALUE / 2;
	private final int indexCount;
	private int stateCount;
	private SubsetList subsets;
	
	public IndexPartitions(StaticIndexSet indexes) {
		if (indexes.size() > MAXIMUM_INDEX_COUNT)
			throw new IllegalArgumentException("Cannot create partitions for " + indexes.size() + "indexes");
		indexCount = indexes.size();
		stateCount = indexes.size() * 2;
		subsets = new SubsetList();
        for (Index index : indexes) {
			subsets.add(new Subset(index)); 
		}
	}

	public IndexPartitions(Snapshot snapshot, BitSet[] partitionBitSets) {
		// create subsets
		int indexCount0 = 0;
		int stateCount0 = 0;
		subsets = new SubsetList();
		for (BitSet bs : partitionBitSets) {
			Subset subset = null;
			for (int i = bs.nextSetBit(0); i >= 0; i = bs.nextSetBit(i+1)) {
                Index idx = snapshot.findIndexId(i);
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

	public BitSet[] bitSetArray() {
		BitSet[] arr = new BitSet[subsets.size()];
		for (int i = 0; i < subsets.size(); i++) {
			arr[i] = subsets.get(i).bitSet();
		}
		return arr;
	}
	
    public final void merge(Index i1, Index i2) {
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

	@Override
	public boolean equals(Object o1) {
		if (!(o1 instanceof IndexPartitions))
			return false;
		
		IndexPartitions other = (IndexPartitions) o1;
		if (indexCount != other.indexCount || stateCount != other.stateCount)
			return false;
		
		return subsets.equals(other.subsets);
	}
	
    public class Subset implements Iterable<Index> {
		private final int sumIndexIds;
		private final int minIndexIds;
		
		private int[] indexIds;
		
        private java.util.TreeMap<Integer,Index> map = new java.util.TreeMap<Integer,Index>();
		
        Subset(Index index) {
			map.put(index.getId(), index);
			indexIds = new int[] { index.getId() };
			sumIndexIds = minIndexIds = index.getId();
		}

		Subset(Subset s1, Subset s2) {
            for (Index idx : s1)
				map.put(idx.getId(), idx);
            for (Index idx : s2)
				map.put(idx.getId(), idx);
			
			indexIds = new int[map.size()];
            { int i = 0; for (Index idx : map.values()) indexIds[i++] = idx.getId(); }
			
			sumIndexIds = s1.sumIndexIds + s2.sumIndexIds;
			minIndexIds = Math.min(s1.minIndexIds, s2.minIndexIds);
		}

        public final boolean contains(Index index) {
			return map.containsKey(index.getId());
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
		
        public java.util.Iterator<Index> iterator() {
			return map.values().iterator();
		}

		public boolean overlaps(Subset other) {
            for (Index x : map.values()) {
				if (other.contains(x))
					return true;
			}
			return false;
		}
		
		@Override
		public boolean equals(Object o) {
			if (!(o instanceof Subset))
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
		
		public BitSet bitSet() {
			BitSet bs = new BitSet();
            for (Index x : this) bs.set(x.getId());
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
			if (!(o instanceof SubsetList))
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
		
        final int whichSubset(Index index) {
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
