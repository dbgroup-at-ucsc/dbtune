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

import edu.ucsc.dbtune.util.IndexBitSet;

import java.sql.SQLException;

/**
 * An immutable type representing a variant of What-if optimizer concept in
 * the dbtune api. This variant is suitable for the IBG-related use cases.
 *
 * @author hsanchez@cs.ucsc.edu (Huascar A. Sanchez)
 */
public abstract class IBGWhatIfOptimizer extends Optimizer {
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
    public abstract double estimateCost(String sql, IndexBitSet configuration, IndexBitSet used) throws SQLException;
}
