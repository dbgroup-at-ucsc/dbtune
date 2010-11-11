package edu.ucsc.satuning.ibg;

import edu.ucsc.satuning.ibg.log.InteractionLogger;
import edu.ucsc.satuning.util.Debug;

public class ThreadIBGAnalysis implements Runnable {
	private IBGAnalyzer         analyzer;
	private InteractionLogger   logger;
    private RunnableState       state;

    private final Object taskMonitor = new Object();

    /**
     * construct a {@code runnable} which will analyze an
     * {@link IndexBenefitGraph}.
     */
    public ThreadIBGAnalysis() {
        this(null, null, RunnableState.IDLE);
	}

    /**
     * a package-private constructor which will construct an instance of
     * the {@code ThreadIBGAnalysis} object.
     * @param analyzer
     *      and {@link IBGAnalyzer} instance.
     * @param logger
     *      and {@link InteractionLogger} instance.
     * @param state
     *      either {@link RunnableState#IDLE}, {@link RunnableState#IDLE}, and
     *      {@link RunnableState#PENDING}, or {@link RunnableState#DONE}. 
     */
    ThreadIBGAnalysis(IBGAnalyzer analyzer, InteractionLogger logger, RunnableState state){
        this.analyzer = analyzer;
        this.logger   = logger;
        this.state    = state;
    }
	
	public void run() {
		while (true) {
			synchronized (taskMonitor) {
				while (state != RunnableState.PENDING) {
					try {
						taskMonitor.wait();
					} catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
						Debug.logError("InterruptedException", e);
					}
				}
			}
			
			int analyzedCount = 0;
			
			boolean done = false;
			while (!done) {
				switch (analyzer.analysisStep(logger, true)) {
					case SUCCESS:
						if (++analyzedCount % 1000 == 0) Debug.println("a" + analyzedCount);
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
				state = RunnableState.DONE;
				taskMonitor.notify();
			}
		}
	}
	
	/**
	 * tell the analysis thread to start analyzing, and return immediately
     * @param analyzer
     *      and {@link IBGAnalyzer} instance.
     * @param logger
     *      and {@link InteractionLogger} instance.
     */
	public void startAnalysis(IBGAnalyzer analyzer, InteractionLogger logger) {
		synchronized (taskMonitor) {
			if (state == RunnableState.PENDING) {
				Debug.logError("unexpected state in IBG startAnalysis");
			}
			
			this.analyzer = analyzer;
			this.logger = logger;
			this.state = RunnableState.PENDING;
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
	
			analyzer = null;
			logger = null;
			state = RunnableState.IDLE;
		}
	}
}
