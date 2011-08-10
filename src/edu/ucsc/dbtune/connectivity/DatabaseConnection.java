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

package edu.ucsc.dbtune.connectivity;

import edu.ucsc.dbtune.advisor.CandidateIndexExtractor;
import edu.ucsc.dbtune.optimizer.IBGWhatIfOptimizer;
import edu.ucsc.dbtune.optimizer.Optimizer;
import edu.ucsc.dbtune.optimizer.WhatIfOptimizer;

/**
 * A connection to a specific database. {@code DatbaseConnection} objects are obtained by using
 * {@link ConnectionManager#connect()}.
 *
 * @author huascar.sanchez@gmail.com (Huascar A. Sanchez)
 * @see edu.ucsc.dbtune.connectivity.DatabaseSession
 * @see ConnectionManager
 */
public interface DatabaseConnection extends DatabaseSession {

    /**
     * gets the instance of the connection manager that created this connection.
     *
     * @return
     *      the {@link ConnectionManager connection manager} instance that created
     *      this connection.
     * @throws NullPointerException
     *      it will throw a null pointer exception if the connection mananger is null.
     *      this is a normal side effect when the connection has been closed.
     */
    ConnectionManager getConnectionManager();

    /**
     * gets the instance of a database index extractor created for this connection.
     *
     * @return
     *      the {@link CandidateIndexExtractor index extractor} instance created
     *      for this connection.
     * @throws NullPointerException
     *      it will throw a null pointer exception if the index extractor is null.
     *      this is a normal side effect when the connection was already closed.
     */
    CandidateIndexExtractor getIndexExtractor();

    /**
     * returns the instance of the object representing the optmizer of the DBMS the connection is 
     * associated with.
     *
     * @return
     *     the {@link Optimizer} instance created for this connection.
     */
    Optimizer getOptimizer();

    /**
     * gets the instance of what-if optmizer created for this connection.
     *
     * @return
     *     the {@link WhatIfOptimizer what-if optimizer} instance created for
     *     this connection.
     * @throws NullPointerException
     *      it will throw a null pointer exception if the optimizer is null.
     *      this is a normal side effect when the connection was already closed.
     */
    WhatIfOptimizer getWhatIfOptimizer();

    /**
     * gets the instance of the IBG-specific what-if optmizer created for this connection.
     *
     * @return
     *     the {@link IBGWhatIfOptimizer what-if optimizer} instance created for
     *     this connection.
     * @throws NullPointerException
     *      it will throw a null pointer exception if the optimizer is null.
     *      this is a normal side effect when the connection was already closed.
     */
    IBGWhatIfOptimizer getIBGWhatIfOptimizer();

    /**
     * loads a set of index tuning resources, such as new {@link CandidateIndexExtractor} strategy, a new
     * {@link WhatIfOptimizer}, or a new {@link IBGWhatIfOptimizer} strategy after construction this {@code connection}
     * object. Multiple calls of this method won't result in extra loading effort since these resources could be only
     * loaded the first time this connection was created.
     */
    void loadResources();
}
