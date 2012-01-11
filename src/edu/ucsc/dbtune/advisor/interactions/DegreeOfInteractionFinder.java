package edu.ucsc.dbtune.advisor.interactions;

import java.sql.SQLException;
import java.util.Set;

import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.optimizer.PreparedSQLStatement;

/**
 * Computes the degree of interaction among the elements of an index set, with respect to a select 
 * statement.
 *
 * @author Ivo Jimenez
 * @see <a href="http://portal.acm.org/citation.cfm?id=1687766">
 *         Index interactions in physical design tuning: modeling, analysis, and applications
 *      </a>
 */
public interface DegreeOfInteractionFinder
{
    /**
     * Computes the interactions for any two distinct elements contained in {@code indexes}, with 
     * respect to a {@code select} statement.
     *
     * @param q
     *      a prepared statement corresponding to a query
     * @param indexes
     *      the set of indexes that are considered when the interactions are discovered.
     * @return
     *      the interactions found
     * @throws SQLException
     *      if the prepared statement doesn't correspond to a {@link SQLCategory.SELECT select}; if 
     *      an error occurs while executing a {@link PreparedSQLStatement#explain what-if} call on 
     *      the prepared statement.
     */
    InteractionBank degreeOfInteraction(PreparedSQLStatement q, Set<Index> indexes)
        throws SQLException;
}
