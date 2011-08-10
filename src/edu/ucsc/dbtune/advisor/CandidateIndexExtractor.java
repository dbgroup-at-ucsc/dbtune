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

import edu.ucsc.dbtune.connectivity.DatabaseConnection;
import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.optimizer.ExplainInfo;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;

/**
 * An {@code DatabaseIndexExtractor} will look deeply into its crystal ball (i.e., a
 * {@code Profiler}) and suggest optimal
 * indexes or the total cost of what-if optimizations.
 *
 * @author huascar.sanchez@gmail.com (Huascar A. Sanchez)
 * @see edu.ucsc.dbtune.optimizer.ExplainInfo
 * @see edu.ucsc.dbtune.metadata.Index
 */
public interface CandidateIndexExtractor {
    /**
     * preps up the database before recommending indexes.
     * @param connection
     *      a database connection to be adjusted.
     */
    void adjust(DatabaseConnection connection);

    /**
     * recommends a {@link Iterable list} of indexes that could help improve performance, given a
     * SQL query.
     *
     *
     *
     *
     * @param sql
     *      SQL query.
     * @return
     *     a {@link Iterable list} of indexes.
     * @throws java.sql.SQLException
     *      an error has occurred when interacting with a database during
     *      the recommendation process.
     */
	Iterable<Index> recommendIndexes(String sql) throws SQLException;

    /**
     * recommends a {@link Iterable list} of indexes that could help improve performance, given a
     * workload file.
     *
     * @param workloadFile
     *      a file consisting of workload statements to be executed.
     * @return
     *     a {@link Iterable list} of indexes.
     * @throws java.sql.SQLException
     *      an error has occurred when interacting with a database during
     *      the recommendation process.
     * @throws java.io.IOException
     *      an error has occurred when trying to open the workload file.
     */
    Iterable<Index> recommendIndexes(File workloadFile) throws SQLException, IOException;
}
