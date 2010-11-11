package edu.ucsc.satuning.ibg;

import java.sql.SQLException;

import edu.ucsc.satuning.util.Debug;

public class ThreadIBGConstruction implements Runnable {
	private IndexBenefitGraphConstructor<?> ibgCons = null;
	
	private final Object taskMonitor = new Object();
	private RunnableState state = RunnableState.IDLE;

	
	public ThreadIBGConstruction() {
        this(null, RunnableState.IDLE);
	}

    ThreadIBGConstruction(IndexBenefitGraphConstructor<?> ibgCons, RunnableState state){
        this.ibgCons = ibgCons;
        this.state   = state;
    }

    @Override
	public void run() {
		while (true) {
			synchronized (taskMonitor) {
				while (state != RunnableState.PENDING) {
					try {
						taskMonitor.wait();
					} catch (InterruptedException e) {
						Debug.logError("InterruptedException", e);
                        Thread.currentThread().interrupt();
                        return; /// hmmm....
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
				state = RunnableState.DONE;
				taskMonitor.notify();
			}
		}
	}

	/**
	 * tells the construction thread to start constructing an IBG, and returns immediately
	 */
	public void startConstruction(IndexBenefitGraphConstructor<?> ibgCons0) {
		synchronized (taskMonitor) {
			if (state == RunnableState.PENDING) {
				Debug.logError("unexpected state in IBG startConstruction");
			}
			
			ibgCons = ibgCons0;
			state = RunnableState.PENDING;
			taskMonitor.notify();
		}
	}
	
	public void waitUntilDone() {
		synchronized (taskMonitor) {
			while (state == RunnableState.PENDING) {
				try {
					taskMonitor.wait();
				} catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
			}
	
			ibgCons = null;
			state = RunnableState.IDLE;
		}
	}
}
