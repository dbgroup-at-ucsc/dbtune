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

import java.sql.SQLException;

public class ThreadIBGConstruction implements Runnable {
    private final String                    processName;
    private IndexBenefitGraphConstructor<?> ibgCons;
    private RunnableState                   state;

    /**
     * construct a {@code runnable} which will construct an
     * {@link IndexBenefitGraph}.
     */
    public ThreadIBGConstruction() {
        this("IBG Construction", null, RunnableState.IDLE);
    }

    /**
     * construct a {@code runnable} which will construct an
     * {@link IndexBenefitGraph}.
     * @param processName
     *      process name given to this runnable.
     * @param ibgCons
     *     an {@link IndexBenefitGraphConstructor} object.
     * @param state
     *     either {@link RunnableState#IDLE}, {@link RunnableState#PENDING},
     *     or {@link RunnableState#DONE}.
     */
    ThreadIBGConstruction(String processName, IndexBenefitGraphConstructor<?> ibgCons, RunnableState state){
        this.processName = processName;
        this.ibgCons     = ibgCons;
        this.state       = state;
    }

    @Override
    public void run() throws RuntimeException {
        throw new RuntimeException("Not implemented yet");
    }

    /**
     * tells the construction thread to start constructing an IBG, and returns immediately
     * @param ibgCons
     *     an {@link IndexBenefitGraphConstructor} object.
     */
    public void startConstruction(IndexBenefitGraphConstructor<?> ibgCons) throws SQLException {
        boolean success;

        do {
            success = ibgCons.buildNode();
        } while (success);
    }

    /**
     * wait until the thread has finalized doing its job.
     */
    public void waitUntilDone() throws RuntimeException {
        throw new UnsupportedOperationException("Not implemented yet");
    }
}
