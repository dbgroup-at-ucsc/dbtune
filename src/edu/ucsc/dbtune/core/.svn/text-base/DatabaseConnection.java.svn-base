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

package edu.ucsc.dbtune.core;

/**
 * A connection to a specific database.  {@code DatbaseConnection} objects are obtained by using
 * {@link DatabaseConnectionManager#connect()}.
 *
 * @author huascar.sanchez@gmail.com (Huascar A. Sanchez)
 * @see edu.ucsc.dbtune.core.DatabaseSession
 * @see DatabaseConnectionManager
 * @param <I> the type of {@link edu.ucsc.dbtune.core.DBIndex}.
 */
public interface DatabaseConnection<I extends DBIndex<I>> extends DatabaseSession {
	/**
	 * gets the instance of the connection manager that created this connection.
	 * @return
     *      the {@link DatabaseConnectionManager connection manager} instance that created
     *      this connection.
     * @throws NullPointerException
     *      it will throw a null pointer exception if the connection mananger is null.
     *      this is a normal side effect when the connection has been closed.
	 */
    DatabaseConnectionManager<I> getConnectionManager();

    /**
     * gets the instance of a database index extractor created for this connection.
     * @return
     *      the {@link edu.ucsc.dbtune.core.DatabaseIndexExtractor index extractor} instance created
     *      for this connection.
     * @throws NullPointerException
     *      it will throw a null pointer exception if the index extractor is null.
     *      this is a normal side effect when the connection was already closed.
     */
    DatabaseIndexExtractor<I> getIndexExtractor();

    /**
     * gets the instance of what-if optmizer created for this connection.
     * @return
     *     the {@link DatabaseWhatIfOptimizer what-if optimizer} instance created for
     *     this connection.
     * @throws NullPointerException
     *      it will throw a null pointer exception if the optimizer is null.
     *      this is a normal side effect when the connection was already closed.
     */
    DatabaseWhatIfOptimizer<I> getWhatIfOptimizer();

    /**
     * install both a new {@link edu.ucsc.dbtune.core.DatabaseIndexExtractor} strategy, and a new
     * {@link DatabaseWhatIfOptimizer} strategy after a {@code connection} object
     * was fully created.
     *
     * @param indexExtractor
     *      a new {@link edu.ucsc.dbtune.core.DatabaseIndexExtractor} instance.
     * @param whatIfOptimizer
     *      a new {@link DatabaseWhatIfOptimizer} instance.
     */
    void install(DatabaseIndexExtractor<I> indexExtractor, DatabaseWhatIfOptimizer<I> whatIfOptimizer);

}
