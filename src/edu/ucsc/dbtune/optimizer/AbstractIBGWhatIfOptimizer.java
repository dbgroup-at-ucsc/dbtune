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

package edu.ucsc.dbtune.optimizer;

import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.util.IndexBitSet;
import edu.ucsc.dbtune.util.Objects;
import edu.ucsc.dbtune.util.ToStringBuilder;
import java.sql.SQLException;

/**
 * This class provides a skeletal implementation of the {@link IBGOptimizer}
 * interface to minimize the effort required to implement this interface.
 *
 * @author hsanchez@cs.ucsc.edu (Huascar A. Sanchez)
 */
public abstract class AbstractIBGWhatIfOptimizer extends IBGOptimizer {
    protected final Optimizer delegate;

    /**
     * initialize an {@code AbstractDatabaseWhatIfOptimizer} object.
     * @param delegate
     *      a DBMS-specific implementation of {@link WhatIfOptimizer} type.
     */
    protected AbstractIBGWhatIfOptimizer(Optimizer delegate) {
        super();
        this.delegate = delegate;
        whatIfCount = 0;
    }

    /**
     * @return the IBG-specific What-if optimizer.
     */
    protected AbstractIBGWhatIfOptimizer getDelegate(){
        return Objects.cast(delegate, AbstractIBGWhatIfOptimizer.class);
    }

    @Override
    public double estimateCost(String sql, IndexBitSet configuration, IndexBitSet used) throws SQLException {
        whatIfCount++;
        return estimateCost(sql, getCandidateSet(), configuration, used);
    }

    // a hook method that should be overriden by implementations of this class.
    abstract double estimateCost(String sql, Iterable<Index> candidate, IndexBitSet configuration,IndexBitSet used);

    @Override
    public PreparedSQLStatement explain(String sql) throws SQLException {
        return delegate.explain(sql);
    }

    /**
     * @return a current candidate set after calling {@link #fixCandidates(Iterable)} method.
     */
    public abstract Iterable<Index> getCandidateSet();

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
    public String toString() {
        return new ToStringBuilder<AbstractIBGWhatIfOptimizer>(this)
               .add("candidateSet", getCandidateSet())
               .toString();
    }

}
