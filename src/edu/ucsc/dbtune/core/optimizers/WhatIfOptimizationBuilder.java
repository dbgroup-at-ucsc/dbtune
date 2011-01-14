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

package edu.ucsc.dbtune.core.optimizers;

import edu.ucsc.dbtune.core.DBIndex;
import edu.ucsc.dbtune.util.DefaultBitSet;

/**
 * @author huascar.sanchez@gmail.com (Huascar A. Sanchez)
 */
public interface WhatIfOptimizationBuilder extends WhatIfOptimizationCostBuilder {
    /**
     * use a pair of {@code bit sets} for performing a what-if optimization.
     * @param config
     *      a new {@link DefaultBitSet configuration} instance.
     * @param usedSet
     *      a {@link DefaultBitSet set} of used indexes.
     * @return
     *     {@link WhatIfOptimizationCostBuilder this}
     */
    WhatIfOptimizationCostBuilder using(DefaultBitSet config, DefaultBitSet usedSet);

    /**
     * use a triple of values (i.e., {@link DefaultBitSet configuration},
     * a {@link edu.ucsc.dbtune.core.DBIndex profiledIndex}, and a {@link DefaultBitSet bitset} containing
     * used columns.)
     *
     * @param config
     *     a new {@link edu.ucsc.dbtune.util.DefaultBitSet configuration} instance.
     * @param profiledIndex
     *      a profiled {@link edu.ucsc.dbtune.core.DBIndex} instance.
     * @param usedColumns
     *      a {@link edu.ucsc.dbtune.util.DefaultBitSet set} of used columns.
     * @return
     *      {@link WhatIfOptimizationCostBuilder this}
     */
    WhatIfOptimizationCostBuilder using(DefaultBitSet config, DBIndex profiledIndex, DefaultBitSet usedColumns);
}
