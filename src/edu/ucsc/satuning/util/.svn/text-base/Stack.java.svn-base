package edu.ucsc.satuning.util;

import java.util.Arrays;

public class Stack<E> {
	Object[] arr;
	int top;
    private static final int DEFAULT_INITIAL_CAPACITY = 100;

	public Stack() {
		arr = new Object[DEFAULT_INITIAL_CAPACITY];
		top = -1;
	}
	
	public final void popAll() {
        while(!isEmpty()){
            pop();
        }
	}
	
	public final void pop() {
        popAndRelease();
	}

    Object popAndRelease(){
		if (top == -1)
			throw new ArrayIndexOutOfBoundsException("cannot pop");
		final Object result = arr[top--];
        arr[top+1] = null; // Eliminate obsolete reference
        return result;
    }

    /**
     * Ensure space for at least one more element, roughly
     * doubling the capacity each time the array needs to grow.
     */
    private void ensureCapacity(){
		if (top == arr.length-1) {
            arr = Arrays.copyOf(arr, arr.length * 2);
		}
    }

	public final E peek() {
        //noinspection RedundantTypeArguments
        return Objects.<E>as(arr[top]);
	}
	
	public final void push(E elt) {
        ensureCapacity();
		arr[++top] = elt;
	}
	
	public final boolean isEmpty() {
		return top == -1;
	}

	public void swap(E next) {
		arr[top] = next;
	}
}
