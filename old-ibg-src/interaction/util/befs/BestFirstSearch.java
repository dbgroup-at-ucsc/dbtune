package interaction.util.befs;

import java.util.Random;

public class BestFirstSearch<T> {
	// the workers
	private final Worker<T>[] workers;
	private final int workerCount;
	
	// for selecting a random worker
	ThreadLocal<Random> rand = new ThreadLocal<Random>() 
		{ public Random initialValue() { return new Random(); }	};
	
	// for synchronizing termination upon exhaustion of tasks
	private int waitCount; 
	private volatile boolean done;
	private Object terminateLock;
	
	public BestFirstSearch(Worker<T>[] workers) {
		this.workers = workers;
		this.workerCount = workers.length;
		this.terminateLock = new Object();
		synchronized (terminateLock) {
			this.waitCount = 0;
			this.done = false;
		}
	}
	
	public void run(T[] tasks) {
		for (T task : tasks)
			addTask(task);
		for (Worker<T> worker : workers)
			worker.start(this);
		for (Worker<T> worker : workers)
			try { worker.join(); } // should not be interrupted
			catch (InterruptedException e) { throw new Error(e); }
	}
	
	/* 
	 * Add a task to the work queue.
	 * 
	 * The given task is assigned to a random worker and they are 
	 * interrupted.
	 */
	protected void addTask(T task) {
		int w = rand.get().nextInt(workerCount);
		workers[w].queue().put(task);
		workers[w].interrupt();
	}

	/*
	 * Get a task that is assigned to the worker, blocking until 
	 * one is available.
	 */
	protected T getTask(Worker<T> worker) throws TerminateSignal {
		while (true) { // repeatedly try to get a problem
			if (done) throw new TerminateSignal();
			T task = worker.queue().get();
			if (task != null) {
				Thread.interrupted(); // clear interrupted status
				return task;
			}
			else waitForInterrupt();
		}
	}
	
	/*
	 * Returns normally if the current thread is interrupted.
	 * Otherwise, throws a TerminateSignal.
	 */
	private void waitForInterrupt() throws TerminateSignal {
		synchronized (terminateLock) {
			waitCount++;			
			if (workerCount == waitCount) { 
				// all other threads sleeping! check for existing task
				boolean foundTask = false;
				for (Worker<? extends Object> worker : workers) {
					if (!worker.queue().isEmpty()) {
						foundTask = true;
						break;
					}
				}
				if (!foundTask) terminate();
			}
			
			// sleep until notification or interrupt
			try { terminateLock.wait();	} 
			catch (InterruptedException signal) { }

			waitCount--;
			if (done) throw new TerminateSignal();
		}
	}

	/*
	 * Force termination before tasks are exhausted
	 */
	public void terminate() throws TerminateSignal {
		synchronized (terminateLock) {
			done = true;
			terminateLock.notifyAll();
			throw new TerminateSignal();
		}
	}
}
