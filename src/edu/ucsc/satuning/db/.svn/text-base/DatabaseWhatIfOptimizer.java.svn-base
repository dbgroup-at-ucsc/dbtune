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
package edu.ucsc.satuning.db;

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
     * @throws SQLException
     *      an error has occurred when building/running a what-if optimization scenario.
     */
    WhatIfOptimizationBuilder<I> whatIfOptimize(String sql) throws SQLException;
}
