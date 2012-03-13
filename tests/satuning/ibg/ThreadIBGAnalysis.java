package satuning.ibg;

import satuning.ibg.log.InteractionLogger;
import satuning.util.Debug;

public class ThreadIBGAnalysis implements Runnable {
	private IBGAnalyzer analyzer = null;
	private InteractionLogger logger = null;
	
	private Object taskMonitor = new Object();
	private State state = State.IDLE;
	
	private enum State { IDLE, PENDING, DONE };
	
	public ThreadIBGAnalysis() {
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
			
			boolean done = false;
			while (!done) {
				switch (analyzer.analysisStep(logger, true)) {
					case SUCCESS:
						break;
					case DONE:
						done = true;
						break;
					case BLOCKED:
						Debug.logError("unexpected BLOCKED result from analysisStep");
						return;
					default:
						Debug.logError("unexpected result from analysisStep");
						return;
				}
			}
			
			synchronized (taskMonitor) {
				state = State.DONE;
				taskMonitor.notify();
			}
		}
	}
	
	/*
	 * tell the analysis thread to start analyzing, and return immediately
	 */
	public void startAnalysis(IBGAnalyzer analyzer0, InteractionLogger logger0) {
		synchronized (taskMonitor) {
			if (state == State.PENDING) {
				Debug.logError("unexpected state in IBG startAnalysis");
			}
			
			analyzer = analyzer0;
			logger = logger0;
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
	
			analyzer = null;
			logger = null;
			state = State.IDLE;
		}
	}
}
