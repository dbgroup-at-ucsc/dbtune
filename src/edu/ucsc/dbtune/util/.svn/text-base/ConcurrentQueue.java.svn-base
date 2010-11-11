package edu.ucsc.dbtune.util;


public class ConcurrentQueue<E> {
	private int cap;
	private Object[] arr;
	private int count;
	private int first;
	
	public enum PutOption {
		// expand the capacity if it's full
		EXPAND {
			void handle(ConcurrentQueue<?> queue) {
				queue.expandInternal();
			}
		},
		// drop the oldest element if it's full
		POP { 
			void handle(ConcurrentQueue<?> queue) {
				queue.popInternal();
			}
		},
		// wait until it's empty if it's full
		BLOCK {
			void handle(ConcurrentQueue<?> queue) {
				queue.waitUntilFull();
			}
		};
		
		abstract void handle(ConcurrentQueue<?> queue);
	}
	
	public enum GetOption {
		// throw NoSuchElementException if it's empty
		THROW {
			void handle(ConcurrentQueue<?> queue) {
				throw new java.util.NoSuchElementException();
			}
		},
		// wait if it's empty
		BLOCK {
			void handle(ConcurrentQueue<?> queue) {
				queue.waitUntilNonempty();
			}
		};
		
		abstract void handle(ConcurrentQueue<?> queue);
	}
	
	public ConcurrentQueue(int cap0) {
		arr = new Object[cap0];
		cap = cap0;
		count = 0;
	}

	public synchronized void put(E elt, PutOption option) {
		if (count >= cap)
			option.handle(this);

		if (count >= cap) {
			Debug.logError("too many elements in queue");
			return;
		}
		
		pushInternal(elt);
		notify();
	}
	
	public synchronized E get(GetOption option) {
		if (count == 0) 
			option.handle(this);

		if (count <= 0) {
			Debug.logError("no elements in queue");
			throw new java.util.NoSuchElementException();
		}	
		
		E elt = peekInternal();
		popInternal();
		notify();
		return elt;
	}
	
	@SuppressWarnings("unchecked")
	private final E peekInternal() { 
		return (E) arr[first]; 
	}
	
	@SuppressWarnings("unchecked")
	private final E peekInternal(int i) { 
		return (E) arr[(first+i) % cap]; 
	}

	private final void pushInternal(E elt) {
		arr[(first+count) % cap] = elt;
		++count;
	}
	
	private final void popInternal() {
		first = (first+1) % cap;
		--count;
	}
	
	private final void expandInternal() {
		int cap2 = cap*2;
		Object[] arr2 = new Object[cap2];
		for (int i = 0; i < count; i++)
			arr2[i] = peekInternal(i);
		first = 0;
		cap = cap2;
		arr = arr2;
	}
	
	private final void waitUntilFull() {
		while (count >= cap) 
			try { wait(); } 
			catch (InterruptedException e) { }
	}
	
	private final void waitUntilNonempty() {
		while (count <= 0) 
			try { wait(); } 
			catch (InterruptedException e) { }
	}
}
