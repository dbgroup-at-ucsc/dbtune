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

import edu.ucsc.dbtune.util.Checks;
import edu.ucsc.dbtune.util.ToStringBuilder;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static edu.ucsc.dbtune.util.Instances.newFalseBoolean;
import static edu.ucsc.satuning.util.Util.newAtomicReference;

/**
 * This class provides a skeletal implementation of the {@link DatabaseConnection}
 * interface to minimize the effort required to implement this interface.
 *
 * @author hsanchez@cs.ucsc.edu (Huascar A. Sanchez)
 */
abstract class AbstractDatabaseConnection extends AbstractDatabaseSession
implements DatabaseConnection {
    private final AtomicReference<ConnectionManager>            connectionManager;
    private final AtomicReference<IndexExtractor>               indexExtractor;
    private final AtomicReference<IBGWhatIfOptimizer>           ibgWhatIfOptimizer;
    private final AtomicReference<WhatIfOptimizer>              optimizer;

    private final AtomicBoolean                                 once;

    /**
     * construct an abstract database connection object.
     * @param connection
     *      a java.sql.Connection
     */
    protected AbstractDatabaseConnection(
            Connection connection){
        super(connection);
        this.connectionManager  = newAtomicReference();
        this.indexExtractor     = newAtomicReference();
        this.ibgWhatIfOptimizer = newAtomicReference();
        this.optimizer          = newAtomicReference();
        this.once               = newFalseBoolean();
    }

    @Override
    void cleanup() throws SQLException {
        super.cleanup();
        getConnectionManager().close(this);
        indexExtractor.set(null);
        optimizer.set(null);
        ibgWhatIfOptimizer.set(null);
        connectionManager.set(null);
    }

    protected void createdBy(ConnectionManager connectionManager){
        this.connectionManager.set(connectionManager);
    }

    protected DatabaseSystem getDatabaseSystem() {
        return getConnectionManager().getDatabaseSystem();
    }

    @Override
    public final void loadResources() {
        if(once.get()) return;
        final DatabaseSystem trait = getDatabaseSystem();
        indexExtractor.set(trait.getIndexExtractor(this));
        optimizer.set(trait.getSimplifiedWhatIfOptimizer(this));
        ibgWhatIfOptimizer.set(trait.getIBGWhatIfOptimizer(this));
        once.set(true);
    }

    @Override
    public ConnectionManager getConnectionManager() {
        return Checks.checkNotNull(connectionManager.get());
    }

    @Override
    public IndexExtractor getIndexExtractor() {
        return Checks.checkNotNull(indexExtractor.get());
    }

    @Override
    public WhatIfOptimizer getWhatIfOptimizer() {
        return Checks.checkNotNull(optimizer.get());
    }

    @Override
    public IBGWhatIfOptimizer getIBGWhatIfOptimizer() {
        return Checks.checkNotNull(ibgWhatIfOptimizer.get());
    }


    @Override
    public String toString() {
        return new ToStringBuilder<AbstractDatabaseConnection>(this)
                .add("connectionManager", connectionManager.get())
                .add("indexExtractor", indexExtractor.get())
                .add("ibgWhatIfOptimizer", ibgWhatIfOptimizer.get())
                .add("simplifiedWhatIfOptimizer", optimizer.get())
                .add("open", isOpened())
                .toString();
    }
}