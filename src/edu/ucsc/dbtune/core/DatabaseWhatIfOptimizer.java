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

import java.sql.SQLException;

/**
 * @author huascar.sanchez@gmail.com (Huascar A. Sanchez)
 */
public interface DatabaseWhatIfOptimizer<I extends DBIndex<I>> {
    /**
     * disable {@code WhatIfOptimizer} once <em>its</em> {@code DatabaseConnection} has been closed.
     */
    void disable();

    /**
     * provides information dealing with the {@link edu.ucsc.dbtune.core.ExplainInfo#maintenanceCost(edu.ucsc.dbtune.core.DBIndex) maintenance
     * cost} associated with the use of a given index.
     *
     * @param sql
     *      sql query.
     * @return
     *      a new {@link edu.ucsc.dbtune.core.ExplainInfo} instance.
     * @throws java.sql.SQLException
     *      an error has occurred when interacting with a database during
     *      the explanation process.
     */
	ExplainInfo<I> explainInfo(String sql) throws SQLException;

    /**
	 * This function prepares the database for what-if optimizations with the
	 * given set of candidates (i.e., indexes). Each call to whatifOptimize is based on the
	 * candidates indicated by this function.
     *
     * @param candidateSet
     *      a set of candidate indexes.
     * @throws java.sql.SQLException
     *      an error has occurred when interacting with a database during
     *      the fixing of candidates.
     */
    void fixCandidates(Iterable<I> candidateSet) throws SQLException;

    /**
     * gets the total count of what-if optimizations were handled/performed
     * by the {@link DatabaseIndexExtractor}
     * @return
     *     the total count of performed what-if optimizations.
     */
    int getWhatIfCount();

    /**
     * calculates the cost of what-if optimization given a workload (sql query).
     * @param sql
     *      experiment's query needed for calculating the cost of what-if optimization.
     * @return
     *      the total cost of the optimization.
     * @throws java.sql.SQLException
     *      an error has occurred when building/running a what-if optimization scenario.
     */
    WhatIfOptimizationBuilder<I> whatIfOptimize(String sql) throws SQLException;
}
