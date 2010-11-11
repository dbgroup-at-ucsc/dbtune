package edu.ucsc.satuning.engine.selection;

import edu.ucsc.satuning.db.DBIndex;

public class StaticIndexSet<I extends DBIndex<I>> implements Iterable<I> {
	private Object[] arr;
	
	public StaticIndexSet(Iterable<I> input) {
		int count = 0;
		for (I idx : input) {
			if (idx == null)
				throw new IllegalArgumentException();
			++count;
		}
		arr = new Object[count];
		int i = 0;
		for (I idx : input)
			arr[i++] = idx;
	}
	
	public StaticIndexSet() {
		arr = new Object[0];
	}

	public boolean contains(I index) {
		if (arr == null)
			return false;
		
		for (Object other : arr) {
			if (index.equals(other)) return true;
		}
		
		return false;
	}

	public int size() {
		return arr.length;
	}
	
	public java.util.Iterator<I> iterator() {
		return new Iterator();
	}
	
	private class Iterator implements java.util.Iterator<I> {
		private int i = 0;
		
		public boolean hasNext() {
			return i < arr.length;
		}

		@SuppressWarnings("unchecked")
		public I next() {
			if (i >= arr.length)
				throw new java.util.NoSuchElementException();
			return (I) arr[i++];
		}

		public void remove() {
			throw new UnsupportedOperationException();
		}
	}
}
