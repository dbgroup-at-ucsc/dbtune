package satuning.ibg;

import java.sql.SQLException;

import satuning.util.Debug;

public class ThreadIBGConstruction implements Runnable {
	private IndexBenefitGraphConstructor ibgCons = null;
	
	private Object taskMonitor = new Object();
	private State state = State.IDLE;

	private enum State { IDLE, PENDING, DONE };
	
	public ThreadIBGConstruction() {
	}

	public void run() {
		while (true) {
			synchronized (taskMonitor) {
				while (state != State.PENDING) {
					try {
						taskMonitor.wait();
					} catch (InterruptedException e) {
						Debug.logError("InterruptedException", e);
					}
				}
			}
			
			try {
				boolean success; // bad name for this variable, should be called "moreNodesToBuild" or something to that effect
				do {
					success = ibgCons.buildNode();
				} while (success);
			} catch (SQLException e) {
				Debug.logError(" *** IBG construction failed ***", e);
			}
			
			synchronized (taskMonitor) {
				state = State.DONE;
				taskMonitor.notify();
			}
		}
	}

	/*
	 * tells the construction thread to start constructing an IBG, and returns immediately
	 */
	public void startConstruction(IndexBenefitGraphConstructor ibgCons0) {
		synchronized (taskMonitor) {
			if (state == State.PENDING) {
				Debug.logError("unexpected state in IBG startConstruction");
			}
			
			ibgCons = ibgCons0;
			state = State.PENDING;
			taskMonitor.notify();
		}
	}
	
	public void waitUntilDone() {
		synchronized (taskMonitor) {
			while (state == State.PENDING) {
				try {
					taskMonitor.wait();
				} catch (InterruptedException e) { }
			}
	
			ibgCons = null;
			state = State.IDLE;
		}
	}
}
