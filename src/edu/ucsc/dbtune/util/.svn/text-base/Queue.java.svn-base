package edu.ucsc.dbtune.util;

import java.util.Arrays;

public class Queue<E> {
	private Object[] arr;
	private int count;
	private int first;
    private static final int DEFAULT_INITIAL_CAPACITY = 100;
	
	public Queue() {
		arr = new Object[DEFAULT_INITIAL_CAPACITY];
		count = 0;
	}
	
	public void add(E elt) {
        ensureCapacity();
		setInternal(count, elt);
		++count;
	}

    /**
     * Ensure space for at least one more element, roughly
     * doubling the capacity each time the array needs to grow.
     */
    private void ensureCapacity(){
		if (count == arr.length) {
			Object[] arr2 = new Object[arr.length * 2];
			for (int i = 0; i < count; i++) {
				arr2[i] = getInternal(i);
			}
			first = 0;
			arr = arr2;
		}
    }

	public E remove() {
		if (count == 0)
			return null;
		
		E ret = getInternal(0);
        setInternal(0, null); // this must be called to reset the "first" attribute.
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
		while(!isEmpty()){
            remove();
        }
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
        final boolean empty = count == 0;
        if(empty) first = 0;
		return empty;
	}
	
	private E getInternal(int i) {
        //noinspection RedundantTypeArguments
        return Objects.<E>as(arr[(first+i) % arr.length]);
	}

	private void setInternal(int i, E elt) {
		arr[(first+i) % arr.length] = elt;
	}

    @Override
    public String toString() {
        return Arrays.toString(arr);
    }
}
