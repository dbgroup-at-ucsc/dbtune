package satuning.util;

public class MinQueue<E> {
	private double[] priorities;
	private Object[] elements;
	private int size;
	
	/* make an empty heap, with a fixed capacity */
	public MinQueue(int cap) {
		priorities = new double[cap+1];
		elements = new Object[cap+1];
		size = 0;
	}
	
	/* make a heap for the given elements, which begin with priority zero */
	public MinQueue(E[] array) {
		size = array.length;
		priorities = new double[size+1];
		elements = new Object[size+1];
		for (int i = 0; i < size; i++) 
			elements[i+1] = array[i];
	}
	
	private final int parent(int i) {
		return i / 2;
	}
	
	private final int right(int i) {
		return 1 + i * 2;
	}
	
	private final int left(int i) {
		return i * 2;
	}
	
	private final void set(int i, double priority, Object elt) {
		priorities[i] = priority;
		elements[i] = elt;
	}
	
	@SuppressWarnings("unchecked")
	private final E getElement(int i) { return (E) elements[i]; }
	
	public void insertKey(E elt, double priority) {
		int i;
		
		assert(priority <= Double.MAX_VALUE);
		
		size++;
		i = size;
		
		while (i > 1) {
			int parent = parent(i);
			
			if (priority >= priorities[parent])
				break;
			else {
				set(i, priorities[parent], elements[parent]);
				i = parent;
			}
		}
		
		set(i, priority, elt);
	}
	
	public E deleteMin() {
		E minElement;
		int i, last;
		
		if (size == 0)
			return null;
		
		minElement = getElement(1);
		
		last = size;
		size--;
		
		i = 1;
		while (true) {
			int left, right, larger;
			
			left = left(i);
			right = right(i);
			
			if (right > size) {
				if (left > size) {
					break;
				}
				else {
					larger = left;
				}
			}
			else if (priorities[left] < priorities[right]) 
				larger = left;
			else
				larger = right;
			
			if (priorities[last] <= priorities[larger]) {
				break;
			}
			else {
				set(i, priorities[larger], elements[larger]);
				i = larger;
			}
		}
		
		set(i, priorities[last], elements[last]);
		
		return minElement;
	}

	public final E peekMin() {
		if (size == 0)
			return null;
		else 
			return getElement(1);
	}

	public final double minPriority() {
		if (size == 0)
			return Double.POSITIVE_INFINITY;
		else 
			return priorities[1];
	}
		
	public final int size() {
		return size;
	}
	
	public final void clear() {
		size = 0;
	}
}