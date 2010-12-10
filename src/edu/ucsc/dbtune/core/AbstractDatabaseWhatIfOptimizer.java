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
import edu.ucsc.dbtune.util.PreConditions;
import edu.ucsc.dbtune.util.ToStringBuilder;

import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static edu.ucsc.dbtune.util.Instances.newAtomicReference;
import static edu.ucsc.dbtune.util.Instances.newTrueBoolean;
import static edu.ucsc.dbtune.util.Objects.as;

/**
 * This class provides a skeletal implementation of the {@link DatabaseWhatIfOptimizer}
 * interface to minimize the effort required to implement this interface.
 *
 * @author huascar.sanchez@gmail.com (Huascar A. Sanchez)
 */
public abstract class AbstractDatabaseWhatIfOptimizer<I extends DBIndex<I>> implements DatabaseWhatIfOptimizer<I> {
    private final AtomicReference<WhatIfOptimizationBuilderImpl<I>> optimizer;
    private final AtomicBoolean                                     enabled;

    protected AbstractDatabaseWhatIfOptimizer(){
        optimizer = newAtomicReference();
        enabled   = newTrueBoolean();
    }

    @Override
    public void disable(){
        enabled.set(false);
    }

    /**
     * @return a current candidate set after calling {@link #fixCandidates(Iterable)} method.
     */
    public abstract Iterable<I> getCandidateSet();

    /**
     * runs n what-if optimizations and return n results (i.e., optimization cost)
     * @throws java.sql.SQLException
     *      unable to calculate costs b/c a database error.
     */
    public void calculateOptimizationCost() throws SQLException {
        PreConditions.checkSQLRelatedState(
                getWhatIfOptimizationBuilder() != null,
                "We cannot calculate optimization without an active(built) optimizer's configuration."
        );
        final WhatIfOptimizationBuilderImpl<I> each = as(getWhatIfOptimizationBuilder());
        each.addCost(runWhatIfTrial(each));
    }

    /**
     * increment whatIfCount
     */
    protected abstract void incrementWhatIfCount();

    /**
     * @return the recently created {@code WhatIfOptimizationBuilder} (scenarios).
     */
    public WhatIfOptimizationBuilder<I> getWhatIfOptimizationBuilder(){
        return optimizer.get();
    }

    /**
     * @return {@code true} if we can use this extractor, {@code false} otherwise.
     */
    protected boolean isEnabled(){
        return enabled.get();
    }

    /**
     * runs a what-if trial.
     * @param builder
     *      the input to the trial.
     * @return
     *      the cost that an optimization will produce - if decided to go for it.
     * @throws java.sql.SQLException
     *      unable to run trial b/c a database error.
     */
    protected abstract Double runWhatIfTrial(WhatIfOptimizationBuilder<I> builder) throws SQLException;

    @Override
    public WhatIfOptimizationBuilder<I> whatIfOptimize(String sql) throws SQLException {
        PreConditions.checkSQLRelatedState(
                isEnabled(),
                "We cannot use this extractor; either its owner connection has been closed or we did not retrieve a valid extractor."
        );
        final WhatIfOptimizationBuilderImpl<I> o = new WhatIfOptimizationBuilderImpl<I>(this, sql);
        optimizer.compareAndSet(optimizer.get(), o);
        return o;
    }

    @Override
    public String toString() {
        return new ToStringBuilder<AbstractDatabaseWhatIfOptimizer<?>>(this)
               .add("optimizations", getWhatIfOptimizationBuilder())
               .add("candidateSet", getCandidateSet())
               .toString();
    }
}
