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

/**
 * todo(Huascar) check the real purpose of this type.
 * represents an explained index.
 * @param <I> the type of index.
 */
public interface ExplainInfo<I extends DBIndex<I>> {
    /**
     * gets the maintenance cost of an index.
     * @param index
     *      a {@link edu.ucsc.dbtune.core.DBIndex} object.
     * @return
     *      maintenance cost.
     */
	double maintenanceCost(I index);

    /**
     * @return {@code true} if it's
     *      {@link edu.ucsc.satuning.workload.SQLStatement.SQLCategory#DML},
     *      {@code false} otherwise.
     */
    boolean isDML();
}
