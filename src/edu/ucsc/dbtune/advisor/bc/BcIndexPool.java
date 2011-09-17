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

package edu.ucsc.dbtune.advisor.bc;

import edu.ucsc.dbtune.metadata.Configuration;
import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.util.Instances;
import edu.ucsc.dbtune.util.ToStringBuilder;

import java.util.Map;

public class BcIndexPool {
    Map<Integer, BcIndexInfo> map;
    Configuration conf;

    /**
     * construct a {@code BcIndexPool} object from a hot set of indexes.
     * @param hotSet
     *      a hot set of indexes.
     */
    public BcIndexPool(Configuration conf, Configuration hotSet) {
        map = Instances.newHashMap(hotSet.size());
        for (Index idx : hotSet) {
            map.put(conf.getOrdinalPosition(idx), new BcIndexInfo());
        }
    }

    /**
     * Returns the {@code BcIndexInfo} matching an index's id.
     * @param id
     *      index's id.
     * @return the {@code BcIndexInfo} matching an index's id.
     */
    public BcIndexInfo get(int id) {
        return map.get(id);
    }

    @Override
    public String toString() {
        return new ToStringBuilder<BcIndexPool>(this)
               .add("idToBcIndexInfo map", map)
               .toString();
    }
}
