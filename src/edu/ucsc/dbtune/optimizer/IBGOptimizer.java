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

import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.optimizer.IBGOptimizer;
import edu.ucsc.dbtune.optimizer.IBGOptimizer;

import java.sql.SQLException;
import java.util.List;

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
    public int getWhatIfCount() {
        return delegate.getWhatIfCount();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<Index> recommendIndexes(String sql) throws SQLException {
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
    public PreparedSQLStatement explain(String sql, Iterable<? extends Index> indexes)
        throws SQLException
    {
        PreparedSQLStatement info;
        int maxId = -1;

        info = delegate.explain(sql, indexes);

        for(Index idx : indexes) {
            if(idx.getId() > maxId) {
                maxId = idx.getId();
            }
        }

        return new IBGPreparedSQLStatement(
                info, construct(delegate, sql, indexes, maxId), delegate.getWhatIfCount());
    }
}
