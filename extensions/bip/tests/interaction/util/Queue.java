package interaction.util;

public class Queue<E> {
	private Object[] arr;
	private int count;
	private int first;
	
	public Queue() {
		arr = new Object[100];
		count = 0;
	}
	
	public void add(E elt) {
		if (count == arr.length) {
			Object[] arr2 = new Object[arr.length * 2];
			for (int i = 0; i < count; i++) {
				arr2[i] = getInternal(i);
			}
			first = 0;
			arr = arr2;
		}
		
		setInternal(count, elt);
		++count;
	}

	public E remove() {
		if (count == 0)
			return null;
		
		E ret = getInternal(0);
		first = (first+1) % arr.length;
		--count;
		
		return ret;
	}

	public E peek() {
		if (count == 0)
			return null;
		
		return getInternal(0);
	}
	
	public void clear() {
		count = 0;
	}

	public E fetch(int i) {
		if (i >= count)
			throw new ArrayIndexOutOfBoundsException();
		return getInternal(i);
	}

	public void replace(int i, E elt) {
		if (i >= count)
			throw new ArrayIndexOutOfBoundsException();
		setInternal(i, elt);
	}
	
	public int count() {
		return count;
	}

	public boolean isEmpty() {
		return count == 0;
	}
	
	@SuppressWarnings("unchecked")
	private final E getInternal(int i) { 
		return (E) arr[(first+i) % arr.length]; 
	}

	private final void setInternal(int i, E elt) {
		arr[(first+i) % arr.length] = elt;
	}
}
