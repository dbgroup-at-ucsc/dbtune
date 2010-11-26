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

import edu.ucsc.dbtune.util.PreConditions;
import edu.ucsc.dbtune.util.ToStringBuilder;

import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicReference;

import static edu.ucsc.satuning.util.Util.newAtomicReference;

/**
 * This class provides a skeletal implementation of the {@link DatabaseConnection}
 * interface to minimize the effort required to implement this interface.
 *
 * @author huascar.sanchez@gmail.com (Huascar A. Sanchez)
 */
abstract class AbstractDatabaseConnection <I extends DBSystem<I>> extends AbstractDatabaseSession
implements DatabaseConnection<I> {
    private final AtomicReference<DatabaseConnectionManager<I>> connectionManager;
    private final AtomicReference<DatabaseIndexExtractor<I>>    indexExtractor;
    private final AtomicReference<DatabaseWhatIfOptimizer<I>>   whatIfOptimizer;

    /**
     * construct an abstract database connection object.
     * @param connectionManager
     *      connection manager instance.
     */
    protected AbstractDatabaseConnection(
            DatabaseConnectionManager<I> connectionManager
    ){
        super();
        this.connectionManager  = newAtomicReference(connectionManager);
        this.indexExtractor     = newAtomicReference();
        this.whatIfOptimizer    = newAtomicReference();
    }

    @Override
    void cleanup() throws SQLException {
        super.cleanup();
        getConnectionManager().close(this);
        getIndexExtractor().disable();
        getWhatIfOptimizer().disable();
        indexExtractor.set(null);
        whatIfOptimizer.set(null);
        connectionManager.set(null);
    }

    @Override
    public void install(DatabaseIndexExtractor<I> newIndexExtractor, DatabaseWhatIfOptimizer<I> newWhatIfOptimizer) {
        indexExtractor.compareAndSet(indexExtractor.get(), newIndexExtractor);
        whatIfOptimizer.compareAndSet(whatIfOptimizer.get(), newWhatIfOptimizer);
    }

    @Override
    public DatabaseConnectionManager<I> getConnectionManager() {
        return PreConditions.checkNotNull(connectionManager.get());
    }

    @Override
    public DatabaseIndexExtractor<I> getIndexExtractor() {
        return PreConditions.checkNotNull(indexExtractor.get());
    }

    @Override
    public DatabaseWhatIfOptimizer<I> getWhatIfOptimizer() {
        return PreConditions.checkNotNull(whatIfOptimizer.get());
    }


    @Override
    public String toString() {
        return new ToStringBuilder<AbstractDatabaseConnection<?>>(this)
                .add("connectionManager", getConnectionManager())
                .add("indexExtractor", getIndexExtractor())
                .add("whatIfOptimizer", getWhatIfOptimizer())
                .add("open", isOpened())
                .toString();
    }
}