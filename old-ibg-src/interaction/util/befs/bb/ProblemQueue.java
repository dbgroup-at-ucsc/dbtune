package interaction.util.befs.bb;

import interaction.util.befs.TaskQueue;

/*
 * Standard binary heap implementation of a priority queue for B&B problems.
 */
public class ProblemQueue implements TaskQueue<Problem> {
	private static final int INITIAL_CAPACITY = 100;
	
	private int count;
	private Problem[] elts;
	private float[] keys; 
	
	public ProblemQueue() {
		this(INITIAL_CAPACITY);
	}
	
	public ProblemQueue(int initialCapacity) {
		count = 0;
		elts = new Problem[initialCapacity];
		keys = new float[initialCapacity];
	}
	
	public synchronized Problem get() {
		if (count == 0) 
			return null;
		else {
			Problem minElt = elts[1];
			if (count == 1)
				count = 0; 
			else {
				// remove last value, and replace the root with it
				Problem lastElt = elts[count];
				float lastKey = keys[count];
				count--;
				replaceRoot(lastElt, lastKey);
			}
		
			// return min element
			return minElt;
		}
	}
	
	/*
	 * Merge the left and right parts of the tree with an element.
	 * 
	 * When this method returns, the heap contains newElt, as well 
	 * as all current elemtns other than the root. The root element
	 * is ignored and overwritten.
	 */
	private void replaceRoot(Problem newElt, float newKey) { 
		// bubble down gap from min value
		int curPos = 1;
		while (true) {
			int leftPos = curPos * 2;
			if (leftPos > count) // no left child
				break; 
			
			// we have a left child
			float leftKey = keys[leftPos];
			int rightPos = leftPos + 1;
			if (rightPos > count) { // no right child
				if (leftKey > newKey) {
					keys[curPos] = leftKey;
					elts[curPos] = elts[leftPos];
					curPos = leftPos;
				}
				break;
			}
			
			// we have a right child
			float rightKey = keys[rightPos];
			if (leftKey > rightKey && leftKey > newKey) {
				keys[curPos] = leftKey;
				elts[curPos] = elts[leftPos];
				curPos = leftPos;
			}
			else if (rightKey > newKey) {
				keys[curPos] = rightKey;
				elts[curPos] = elts[rightPos];
				curPos = rightPos;
			}
			else break;
		}
		
		elts[curPos] = newElt;
		keys[curPos] = newKey;
	}
	
	public synchronized void put(Problem newElt) {
		if (count == elts.length - 1) growArrays();
		int curPos = ++count;
		
		// bubble up
		float newKey = newElt.lowerBound();
		while (curPos > 1) {
			int parentPos = curPos / 2;
			float parentKey = keys[parentPos]; 
			Problem parentElt = elts[parentPos];
			if (newKey > parentKey) {
				elts[curPos] = parentElt;
				keys[curPos] = parentKey;
				curPos = parentPos;
			}
			else break;
		}
		
		elts[curPos] = newElt;
		keys[curPos] = newKey;
	}
	
	public synchronized boolean isEmpty() {
		return count == 0;
	}
	
	private void growArrays() {
		int newCapacity = elts.length * 2;
		Problem[] newElts = new Problem[newCapacity];
		float[] newKeys = new float[newCapacity];	
		System.arraycopy(elts, 1, newElts, 1, count);
		System.arraycopy(keys, 1, newKeys, 1, count);
		elts = newElts;
		keys = newKeys;
	}
}
