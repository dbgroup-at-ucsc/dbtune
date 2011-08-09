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
import edu.ucsc.dbtune.core.metadata.Index;
import edu.ucsc.dbtune.spi.core.Console;
import edu.ucsc.dbtune.util.Checks;
import edu.ucsc.dbtune.util.IndexBitSet;
import edu.ucsc.dbtune.util.Objects;
import static edu.ucsc.dbtune.util.Objects.cast;
import edu.ucsc.dbtune.util.ToStringBuilder;
import java.sql.SQLException;

/**
 * This class provides a skeletal implementation of the {@link IBGWhatIfOptimizer}
 * interface to minimize the effort required to implement this interface.
 *
 * @author hsanchez@cs.ucsc.edu (Huascar A. Sanchez)
 */
public abstract class AbstractIBGWhatIfOptimizer extends AbstractWhatIfOptimizer implements IBGWhatIfOptimizer {
    private final AbstractWhatIfOptimizer                           delegate;

    /**
     * initialize an {@code AbstractDatabaseWhatIfOptimizer} object.
     * @param delegate
     *      a DBMS-specific implementation of {@link WhatIfOptimizer} type.
     */
    protected AbstractIBGWhatIfOptimizer(WhatIfOptimizer delegate){
        super();
        this.delegate   = cast(Checks.checkNotNull(delegate), AbstractWhatIfOptimizer.class);
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
        incrementWhatIfCount();
        Console.streaming().dot();
        if (getWhatIfCount() % 75 == 0) Console.streaming().skip();
        return estimateCost(sql, getCandidateSet(), configuration, used);
    }

    // a hook method that should be overriden by implementations of this class.
    double estimateCost(String sql, Iterable<Index> candidate, IndexBitSet configuration,
        IndexBitSet used){
      return 0.0;
    }

    @Override
    public double estimateCost(String sql, IndexBitSet configuration, IndexBitSet used, Index profiledIndex) throws SQLException {
        throw new UnsupportedOperationException("AbstractIBGWhatIfOptimizer#estimateCost(..) not supported yet.");
    }

    @Override
    public ExplainInfo explain(String sql) throws SQLException {
        updateCachedSQL(sql);
        return explain(sql, getCandidateSet());
    }

    @Override
    public ExplainInfo explain(Iterable<? extends Index> indexes) throws SQLException {
        return explain(getCachedSQL(), indexes);
    }

    @Override
    public ExplainInfo explain(String sql, Iterable<? extends Index> indexes) throws SQLException {
        return delegate.explain(sql, indexes);
    }

    /**
     * @return a current candidate set after calling {@link #fixCandidates(Iterable)} method.
     */
    public abstract Iterable<Index> getCandidateSet();


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
               .add("candidateSet", getCandidateSet())
               .toString();
    }

}
