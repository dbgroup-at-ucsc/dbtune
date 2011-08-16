/* **************************************************************************** *
 *   Copyright 2010 University of California Santa Cruz                         *
 *                                                                              *
 *   Licensed under the Apache License, Version 2.0 (the "License");            *
 *   you may not use this file except in compliance with the License.           *
 *   You may obtain a copy of the License at                                    *
 *                                                                              *
 *       http://www.apache.org/licenses/LICENSE-2.0                             *
 *                                                                              *
 *   Unless required by applicable law or agreed to in writing, software        *
 *   distributed under the License is distributed on an "AS IS" BASIS,          *
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.   *
 *   See the License for the specific language governing permissions and        *
 *   limitations under the License.                                             *
 * **************************************************************************** */
package edu.ucsc.dbtune.optimizer;

import edu.ucsc.dbtune.metadata.Configuration;
import edu.ucsc.dbtune.metadata.ConfigurationBitSet;
import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.ibg.IndexBenefitGraph;
import edu.ucsc.dbtune.util.IndexBitSet;

import java.sql.SQLException;

import static edu.ucsc.dbtune.ibg.IndexBenefitGraphConstructor.construct;

/**
 * Represents a variant of What-if optimizer concept in the dbtune API.
 *
 * @author Huascar A. Sanchez
 * @author Ivo Jimenez
 */
public class IBGOptimizer extends Optimizer {
    protected Optimizer delegate;

    /**
     * Construct an {@code IBGOptimizer} that relies on the given Optimizer.
     *
     * @param delegate
     *      a DBMS-specific implementation of {@link WhatIfOptimizer} type.
     */
    public IBGOptimizer(Optimizer delegate) {
        this.delegate = delegate;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getOptimizationCount() {
        return delegate.getOptimizationCount();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Configuration recommendIndexes(String sql) throws SQLException {
        return delegate.recommendIndexes(sql);
    }

    /**
     * estimate what-if optimization cost given a single sql statement.
     *
     * @param sql
     *      sql statement
     * @param configuration
     *      an index configuration
     * @return
     *      the prepared statement
     * @throws SQLException
     *      unable to estimate cost due to the stated reasons.
     */
    public PreparedSQLStatement explain(String sql, Configuration configuration)
        throws SQLException
    {
        PreparedSQLStatement stmt;
        ConfigurationBitSet  bitConf;
        IndexBenefitGraph    ibg;
        IndexBitSet          bitSet;

        bitSet  = new IndexBitSet();
        stmt    = delegate.explain(sql, configuration);

        for(Index idx : configuration) {
            bitSet.set(idx.getId());
        }

        bitConf = new ConfigurationBitSet(configuration, bitSet);
        ibg     = construct(delegate, sql, bitConf);

        return new IBGPreparedSQLStatement(stmt, bitConf, ibg, delegate.getOptimizationCount());
    }
}
