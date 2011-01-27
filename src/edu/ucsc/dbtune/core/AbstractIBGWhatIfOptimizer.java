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

import edu.ucsc.dbtune.core.optimizers.WhatIfOptimizationBuilder;
import edu.ucsc.dbtune.util.*;

import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicReference;

import static edu.ucsc.dbtune.spi.core.Commands.supplyValue;
import static edu.ucsc.dbtune.util.Instances.newAtomicInteger;
import static edu.ucsc.dbtune.util.Instances.newAtomicReference;
import static edu.ucsc.dbtune.util.Objects.as;
import static edu.ucsc.dbtune.util.Objects.cast;

/**
 * This class provides a skeletal implementation of the {@link IBGWhatIfOptimizer}
 * interface to minimize the effort required to implement this interface.
 *
 * @author hsanchez@cs.ucsc.edu (Huascar A. Sanchez)
 */
public abstract class AbstractIBGWhatIfOptimizer extends AbstractWhatIfOptimizer implements IBGWhatIfOptimizer {
    private final AtomicReference<WhatIfOptimizationBuilderImpl>    optimizer;
    private final AbstractWhatIfOptimizer                           delegate;

    /**
     * initialize an {@code AbstractDatabaseWhatIfOptimizer} object.
     * @param delegate
     *      a DBMS-specific implementation of {@link WhatIfOptimizer} type.
     */
    protected AbstractIBGWhatIfOptimizer(WhatIfOptimizer delegate){
        super();
        this.delegate   = cast(Checks.checkNotNull(delegate), AbstractWhatIfOptimizer.class);
        optimizer       = newAtomicReference();
    }

    /**
     * runs n what-if optimizations and return n results (i.e., optimization cost)
     * @throws java.sql.SQLException
     *      unable to calculate costs b/c a database error.
     */
    public void calculateOptimizationCost() throws SQLException {
        Checks.checkSQLRelatedState(getWhatIfOptimizationBuilder() != null, "Error: not initialized optimizer.");
        final WhatIfOptimizationBuilderImpl each = as(getWhatIfOptimizationBuilder());
        each.addCost(estimateCost(each));
    }

    /**
     * @return the IBG-specific What-if optimizer.
     */
    protected AbstractIBGWhatIfOptimizer getDelegate(){
        return Objects.cast(delegate, AbstractIBGWhatIfOptimizer.class);
    }

    @Override
    protected DatabaseConnection getConnection() {
        return delegate.getConnection();
    }

    @Override
    public double estimateCost(String sql, IndexBitSet configuration, IndexBitSet used) throws SQLException {
        updateCachedSQL(sql);
        return whatIfOptimize(sql).using(configuration, used).toGetCost();
    }

    @Override
    public double estimateCost(String sql, IndexBitSet configuration, IndexBitSet used, DBIndex profiledIndex) throws SQLException {
        updateCachedSQL(sql);
        return whatIfOptimize(sql).using(configuration, profiledIndex, used).toGetCost();
    }

    @Override
    public ExplainInfo explain(String sql) throws SQLException {
        updateCachedSQL(sql);
        return explain(sql, getCandidateSet());
    }

    @Override
    public ExplainInfo explain(Iterable<? extends DBIndex> indexes) throws SQLException {
        return explain(getCachedSQL(), indexes);
    }

    @Override
    public ExplainInfo explain(String sql, Iterable<? extends DBIndex> indexes) throws SQLException {
        return delegate.explain(sql, indexes);
    }

    /**
     * @return a current candidate set after calling {@link #fixCandidates(Iterable)} method.
     */
    public abstract Iterable<DBIndex> getCandidateSet();

    /**
     * @return the recently created {@code WhatIfOptimizationBuilder} (scenarios).
     */
    public WhatIfOptimizationBuilder getWhatIfOptimizationBuilder(){
        return optimizer.get();
    }

    /**
     * @return {@code true} if the optimizer is disabled. {@code false} otherwise.
     */
    protected boolean isDisabled(){
        return null == getConnection() || getConnection().isClosed();
    }

    /**
     * @return {@code true} if the optimizer is enabled. {@code false} otherwise.
     */
    protected boolean isEnabled(){
        return !isDisabled();
    }

    /**
     * increment whatIfCount
     */
    protected void incrementWhatIfCount(){}

    /**
     * runs a what-if trial.
     *
     * @param builder
     *      the input to the trial.
     * @return
     *      the cost that an optimization will produce - if decided to go for it.
     * @throws java.sql.SQLException
     *      unable to run trial b/c a database error.
     */
    protected abstract double estimateCost(WhatIfOptimizationBuilder builder) throws SQLException;

    @Override
    protected void updateCachedSQL(String sql) {
        delegate.updateCachedSQL(sql);
    }

    @Override
    public String toString() {
        return new ToStringBuilder<AbstractIBGWhatIfOptimizer>(this)
               .add("optimizations", getWhatIfOptimizationBuilder())
               .add("candidateSet", getCandidateSet())
               .toString();
    }

    /**
     * calculates the cost of what-if optimization given a workload (sql query).
     * @param sql
     *      experiment's query needed for calculating the cost of what-if optimization.
     * @return
     *      the total cost of the optimization.
     * @throws java.sql.SQLException
     *      an error has occurred when building/running a what-if optimization scenario.
     */
    public WhatIfOptimizationBuilder whatIfOptimize(String sql) throws SQLException {
        Checks.checkSQLRelatedState(isEnabled(),"Error: connection is closed.");
        final WhatIfOptimizationBuilderImpl o = new WhatIfOptimizationBuilderImpl(this, sql);
        optimizer.compareAndSet(optimizer.get(), o);
        return o;
    }
}
