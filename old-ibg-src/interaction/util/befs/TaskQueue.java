package interaction.util.befs;

public interface TaskQueue<T> {
	public T get();
	public void put(T task);
	public boolean isEmpty();
}
