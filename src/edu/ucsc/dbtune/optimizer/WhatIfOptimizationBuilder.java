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

/**
 * @author huascar.sanchez@gmail.com (Huascar A. Sanchez)
 */
public interface WhatIfOptimizationBuilder extends WhatIfOptimizationCostBuilder {
    /**
     * use a pair of {@code bit sets} for performing a what-if optimization.
     * @param config
     *      a new {@link edu.ucsc.dbtune.util.IndexBitSet configuration} instance.
     * @param usedSet
     *      a {@link edu.ucsc.dbtune.util.IndexBitSet set} of used indexes.
     * @return
     *     {@link WhatIfOptimizationCostBuilder this}
     */
    WhatIfOptimizationCostBuilder using(IndexBitSet config, IndexBitSet usedSet);

    /**
     * use a triple of values (i.e., {@link edu.ucsc.dbtune.util.IndexBitSet configuration},
     * a {@link edu.ucsc.dbtune.metadata.Index profiledIndex}, and a {@link edu.ucsc.dbtune.util.IndexBitSet bitset} 
     * containing used columns.)
     *
     * @param config
     *     a new {@link edu.ucsc.dbtune.util.IndexBitSet configuration} instance.
     * @param profiledIndex
     *      a profiled {@code Index} instance.
     * @param usedColumns
     *      a {@link edu.ucsc.dbtune.util.IndexBitSet set} of used columns.
     * @return
     *      {@link WhatIfOptimizationCostBuilder this}
     */
    WhatIfOptimizationCostBuilder using(IndexBitSet config, Index profiledIndex, IndexBitSet usedColumns);
}
