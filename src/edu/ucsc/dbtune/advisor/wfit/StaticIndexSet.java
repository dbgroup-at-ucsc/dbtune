package edu.ucsc.dbtune.advisor.wfit;

import edu.ucsc.dbtune.metadata.Index;

public class StaticIndexSet implements Iterable<Index> {
    private Index[] arr;
	
    public StaticIndexSet(Index[] arr0) {
        for (Index idx : arr0) {
			if (idx == null)
				throw new IllegalArgumentException();
		}
		arr = arr0;
	}
	
	public StaticIndexSet() {
        arr = new Index[0];
	}

    public boolean contains(Index index) {
		if (arr == null)
			return false;
		
        for (Index other : arr) {
			if (index.equals(other)) return true;
		}
		
		return false;
	}

	public int size() {
		return arr.length;
	}
	
    public java.util.Iterator<Index> iterator() {
		return new Iterator();
	}
	
    private class Iterator implements java.util.Iterator<Index> {
		private int i = 0;
		
		public boolean hasNext() {
			return i < arr.length;
		}

        public Index next() {
			if (i >= arr.length)
				throw new java.util.NoSuchElementException();
			return arr[i++];
		}

		public void remove() {
			throw new UnsupportedOperationException();
		}
	}
}
