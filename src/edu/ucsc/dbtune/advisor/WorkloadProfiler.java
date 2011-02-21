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
package edu.ucsc.dbtune.advisor;

import edu.ucsc.dbtune.core.DBIndex;
import edu.ucsc.dbtune.ibg.CandidatePool.Snapshot;

import java.sql.SQLException;

/**
 * @author huascar.sanchez@gmail.com (Huascar A. Sanchez)
 */
public interface WorkloadProfiler<I extends DBIndex> {
    /**
     * Adds new candidate indexes to the current pool of candidates.
     * @param index
     *      new {@link DBIndex index}.
     * @return
     *      an immutable {@link Snapshot snapshot} of the pool of
     *      candidates.
     * @throws SQLException
     *      an unexpected error has occurred while adding new indexes
     *      to the pool.
     */
    Snapshot<I> addCandidate(I index) throws SQLException;

    /**
     * process a {@code sql} query and returned a profiled version of it.
     * @param sql
     *      plain {@code sql} query.
     * @return
     *      a new {@link ProfiledQuery} instance.
     */
    ProfiledQuery<I> processQuery(String sql);

    /**
     * process a vote on a given index. if the vote is positive then
     * add that index to the pool of candidates.
     * @param index
     *      index which has been cast a vote.
     * @param isPositive
     *      flag that indicates if the vote is positive.
     * @return
     *      an immutable {@link Snapshot snapshot} of the pool of
     *      candidates.
     * @throws SQLException
     *      an unexpected error has occurred while adding new indexes (with positive vote)
     *      to the pool.
     */
    Snapshot<I> processVote(I index, boolean isPositive) throws SQLException;
}
