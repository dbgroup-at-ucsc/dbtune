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

package edu.ucsc.dbtune.core.optimizer;

import edu.ucsc.dbtune.core.metadata.Index;
import edu.ucsc.dbtune.util.IndexBitSet;

import java.sql.SQLException;

/**
 * An immutable type representing a variant of What-if optimizer concept in
 * the dbtune api. This variant is suitable for the IBG-related use cases.
 *
 * @author hsanchez@cs.ucsc.edu (Huascar A. Sanchez)
 */
public interface IBGWhatIfOptimizer extends WhatIfOptimizer {
    /**
	 * This function prepares the database for what-if optimizations with the
	 * given set of candidates (i.e., indexes). Each call to this function requires a
     * a set of candidate indexes.
     * @param candidateSet
     *      a set of candidate indexes.
     * @throws java.sql.SQLException
     *      an error has occurred when interacting with a database during
     *      the fixing of candidates.
     */
    void fixCandidates(Iterable<? extends Index> candidateSet) throws
         SQLException;

    /**
     * gets the total count of what-if optimizations were handled/performed
     * by the {@link IBGWhatIfOptimizer}.
     * @return
     *     the total count of performed what-if optimizations.
     */
    int getWhatIfCount();

    /**
     * estimate what-if optimization cost given a single sql statement.
     * @param sql
     *      sql statement
     * @param configuration
     *      an index configuration
     * @param used
     *      the used indexes in the index configuration.
     * @return the estimated optimization cost.
     * @throws SQLException
     *      unable to estimate cost due to the stated reasons.
     */
    double estimateCost(String sql, IndexBitSet configuration, IndexBitSet used) throws
           SQLException;

    /**
     * estimate what-if optimization cost given a single sql statement.
     * @param sql
     *      sql statement
     * @param configuration
     *      an index configuration
     * @param used
     *      the used indexes in the index configuration.
     * @param profiledIndex
     *      an already profiled {@link Index} instance.
     * @return the estimated optimization cost.
     * @throws java.sql.SQLException
     *      unable to estimate cost due to the stated reasons.
     */
    double estimateCost(String sql, IndexBitSet configuration, IndexBitSet used, Index profiledIndex) throws
           SQLException;
}
