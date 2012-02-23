package interaction.util;

public class Stack<E> {
	Object[] arr;
	int top;
	
	public Stack() {
		arr = new Object[100];
		top = -1;
	}
	
	public final void popAll() {
		top = -1;
	}
	
	public final void pop() {
		if (top == -1)
			throw new ArrayIndexOutOfBoundsException("cannot pop");
		top--;
	}

	@SuppressWarnings("unchecked")
	public final E peek() {
		return (E) arr[top];
	}
	
	public final void push(E elt) {
		if (top == arr.length-1) {
			Object[] temp = new Object[arr.length * 2];
			for (int i = 0; i < arr.length; i++) 
				temp[i] = arr[i];
			arr = temp;
		}
		arr[++top] = elt;
	}
	
	public final boolean isEmpty() {
		return top == -1;
	}

	public void swap(E next) {
		arr[top] = next;
	}
}
