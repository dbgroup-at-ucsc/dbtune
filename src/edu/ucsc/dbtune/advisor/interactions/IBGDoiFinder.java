package edu.ucsc.dbtune.advisor.interactions;

import java.sql.SQLException;
import java.util.Set;

import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.optimizer.IBGPreparedSQLStatement;
import edu.ucsc.dbtune.optimizer.PreparedSQLStatement;

/**
 * Computes the degree of interaction by analyzing prepared statements through the qINTERACT 
 * algorithm described in "Index interactions in physical design tuning: modeling, analysis, and 
 * applications". In short, this implementation checks the type of prepared statements that receives 
 * and it proceeds in the following way:
 * <ul>
 *   <li>if an instance of {@link IBGPreparedSQLStatement}, it retrieves the corresponding{@link 
 *   IndexBenefitGraph} and applies the qINTERACT algorithm</li>
 *   <li>for any other prepared statement implementation, it calls {@link 
 *   DefaultDoiFinder#degreeOfInteraction} </li>
 * </ul>
 *
 * @author Ivo Jimenez
 * @see <a href="http://portal.acm.org/citation.cfm?id=1687766">
 *         Index interactions in physical design tuning: modeling, analysis, and applications
 *      </a>
 */
public class IBGDoiFinder extends DefaultDoiFinder
{
    /**
     * {@inheritDoc}
     */
    public InteractionBank degreeOfInteraction(PreparedSQLStatement sql, Set<Index> s)
        throws SQLException
    {
        if (!(sql instanceof IBGPreparedSQLStatement))
            return super.degreeOfInteraction(sql, s);

        IBGPreparedSQLStatement ibgStmt = (IBGPreparedSQLStatement) sql;

        return IBGAnalyzer.analyze(ibgStmt.getIndexBenefitGraph());
    }
}
