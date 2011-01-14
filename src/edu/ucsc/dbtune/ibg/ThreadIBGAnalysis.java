/*
 * ****************************************************************************
 *   Copyright 2010 University of California Santa Cruz                       *
 *                                                                            *
 *   Licensed under the Apache License, Version 2.0 (the "License");          *
 *   you may not use this file except in compliance with the License.         *
 *   You may obtain a copy of the License at                                  *
 *                                                                            *
 *       http://www.apache.org/licenses/LICENSE-2.0                           *
 *                                                                            *
 *   Unless required by applicable law or agreed to in writing, software      *
 *   distributed under the License is distributed on an "AS IS" BASIS,        *
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. *
 *   See the License for the specific language governing permissions and      *
 *   limitations under the License.                                           *
 *  ****************************************************************************
 */

package edu.ucsc.dbtune.ibg;

import edu.ucsc.dbtune.util.Debug;

public class ThreadIBGAnalysis implements Runnable {
    private final Object taskMonitor = new Object();

    private final String        processName;
    private IBGAnalyzer         analyzer;
    private InteractionLogger   logger;
    private RunnableState       state;

    /**
     * construct a {@code runnable} which will analyze an
     * {@link IndexBenefitGraph index benefit graph}.
     */
    public ThreadIBGAnalysis() {
        this("IBG Analysis", null, null, RunnableState.IDLE);
	}

    /**
     * a package-private constructor which will construct an instance of
     * the {@code ThreadIBGAnalysis} object.
     * @param processName
     *      process name given to this runnable.
     * @param analyzer
     *      and {@link IBGAnalyzer} instance.
     * @param logger
     *      and {@link InteractionLogger} instance.
     * @param state
     *      either {@link RunnableState#IDLE}, {@link RunnableState#IDLE}, and
     *      {@link RunnableState#PENDING}, or {@link RunnableState#DONE}.
     */
    ThreadIBGAnalysis(String processName, IBGAnalyzer analyzer, InteractionLogger logger, RunnableState state){
        this.processName = processName;
        this.analyzer    = analyzer;
        this.logger      = logger;
        this.state       = state;
    }

    @Override
	public void run() {
        System.out.printf("%s is running.\n",processName);        
		while (true) {
			synchronized (taskMonitor) {
				while (!RunnableState.PENDING.isSame(state)) {
					try {
						taskMonitor.wait();
					} catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
						Debug.logNotice("InterruptedException", e);
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
			if (RunnableState.PENDING.isSame(state)) {
                final String msg = "unexpected state in IBG startAnalysis";
				Debug.logError(msg);
                // todo(Huascar) ask whether we want to fail fast (by throwing an illegal state excep. when
                // finding this violation or just leave it the way it is.
			}
			
			this.analyzer   = analyzer;
			this.logger     = logger;
			this.state      = RunnableState.PENDING;
			taskMonitor.notify();
		}
	}
	
	public void waitUntilDone() {
		synchronized (taskMonitor) {
			while (RunnableState.PENDING.isSame(state)) {
				try {
					taskMonitor.wait();
				} catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    Debug.logNotice("interrupted thread", e);
                }
			}
	
			analyzer = null;
			logger   = null;
			state    = RunnableState.IDLE;
		}
	}
}
