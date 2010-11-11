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

package edu.ucsc.dbtune.spi.ibg;

import edu.ucsc.dbtune.core.DBIndex;
import edu.ucsc.dbtune.util.ToStringBuilder;
import edu.ucsc.dbtune.util.BitSet;

public class AnalyzedQuery<I extends DBIndex<I>> {
	private final ProfiledQuery<I> profileInfo;
	private final BitSet[]         partition;

    /**
     * construct a query which has been analyzed by some {@code tuning interface}.
     * @param orig
     *      original query before it got analyzed.
     * @param partition
     *      an array of index partitions.
     */
	public AnalyzedQuery(ProfiledQuery<I> orig, BitSet[] partition) {
		this.profileInfo    = orig;
		this.partition      = partition;
	}

    /**
     * @return original query before it got analyzed.
     */
    public ProfiledQuery<I> getProfileInfo() {
        return profileInfo;
    }

    /**
     * @return an array of index partitions.
     */
    public BitSet[] getPartition() {
        return partition;
    }

    @Override
    public String toString() {
        return new ToStringBuilder<AnalyzedQuery<I>>(this)
               .add("original", profileInfo)
               .add("partition", partition)
               .toString();
    }
}
