package edu.ucsc.dbtune.core;

import edu.ucsc.dbtune.core.metadata.Index;
import java.sql.SQLException;

/**
 * An immutable type representing the What-if optimizer concept in
 * the dbtune api.
 *
 * @author hsanchez@cs.ucsc.edu (Huascar A. Sanchez)
 */
public interface WhatIfOptimizer {

    /**
     * perform a what-if optimization call over a single sql statement.
     * @param sql
     *      sql statement
     * @return an {@link ExplainInfo} object describing the results of a
     *      what-if optimization call. e.g., the maintenance cost of indexes that could
     *      potentially be materialized.
     * @throws SQLException
     *      unable to explain the sql for the stated reasons.
     */
    ExplainInfo explain(String sql) throws SQLException;

    /**
     * perform a what-if optimization call over a container (i.e., iterable container)
     * of indexes.
     * @param indexes
     *      iterable container of indexes.
     * @return an {@link ExplainInfo} object describing the results of a
     *      what-if optimization call. e.g., the maintenance cost of indexes that could
     *      potentially be materialized.
     * @throws java.sql.SQLException
     *      unable to explain the container of indexes for the stated reasons.
     */
    ExplainInfo explain(Iterable<? extends Index> indexes) throws SQLException;

    /**
     * estimate what-if optimization cost of a container (i.e., iterable container)
     * of indexes.
     * @param sql
     *      sql statement
     * @param indexes
     *      iterable container of indexes.
     * @return an {@link ExplainInfo} object describing the results of a
     *      what-if optimization call. e.g., the maintenance cost of indexes that could
     *      potentially be materialized.
     * @throws java.sql.SQLException
     *      unable to estimate cost for the stated reasons.
     */
    ExplainInfo explain(String sql, Iterable<? extends Index> indexes) throws SQLException;
}
