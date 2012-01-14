package interaction.util.befs;

public abstract class Worker<T> implements Runnable, Emit<T> {
	private BestFirstSearch<T> befs;
	private Thread thread = new Thread(this);;
	
	/* Fork a thread for this worker, under control of given engine */
	public final void start(BestFirstSearch<T> eng) {
		befs = eng;
		thread.start();
	}

	/* Join this worker's thread. */
	public final void join() throws InterruptedException {
		thread.join();
	}
	
	/* Interrupt this worker's thread. */
	public final void interrupt() {
		thread.interrupt();
	}
	
	/* Force engine will stop all threads. May be called at any time. */
	public void terminate() throws TerminateSignal {
		befs.terminate();
	}
	
	/*
	 * Main execution loop. Fetch tasks and call abstract "process" method
	 * until a TerminateSignal is caught.
	 */
	public final void run() {
		while (true) {
			try { process(befs.getTask(this)); }
			catch (TerminateSignal sig) { break; }
		}
	}
	
	/*
	 * A subclass can write an emit method with filtering logic, and
	 * call this method if the filter passes
	 */
	public void emit(T task) {
		befs.addTask(task);
	}
	
	/*
	 * Do application-specific processing of the task. The 
	 * worker may emit subproblems and/or terminate the engine.
	 */
	public abstract void process(T task) throws TerminateSignal;
	
	/*
	 * Export a reference to the worker's queue
	 */
	public abstract TaskQueue<T> queue();
}
