package satuning.util;

public class BlockingQueue<E> extends satuning.util.ConcurrentQueue<E> {
	public BlockingQueue(int capacity) {
		super(capacity);
	}

	public E get() {
		return super.get(GetOption.BLOCK);
	}

	public void put(E elt) {
		super.put(elt, PutOption.BLOCK);
	}
}
