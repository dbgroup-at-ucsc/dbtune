package satuning.engine.selection;

import satuning.db.DB2Index;

public class StaticIndexSet implements Iterable<DB2Index> {
	private DB2Index[] arr;
	
	public StaticIndexSet(DB2Index[] arr0) {
		for (DB2Index idx : arr0) {
			if (idx == null)
				throw new IllegalArgumentException();
		}
		arr = arr0;
	}
	
	public StaticIndexSet() {
		arr = new DB2Index[0];
	}

	public boolean contains(DB2Index index) {
		if (arr == null)
			return false;
		
		for (DB2Index other : arr) {
			if (index.equals(other)) return true;
		}
		
		return false;
	}

	public int size() {
		return arr.length;
	}
	
	public java.util.Iterator<DB2Index> iterator() {
		return new Iterator();
	}
	
	private class Iterator implements java.util.Iterator<DB2Index> {
		private int i = 0;
		
		public boolean hasNext() {
			return i < arr.length;
		}

		public DB2Index next() {
			if (i >= arr.length)
				throw new java.util.NoSuchElementException();
			return arr[i++];
		}

		public void remove() {
			throw new UnsupportedOperationException();
		}
	}
}
