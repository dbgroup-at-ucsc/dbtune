package edu.ucsc.dbtune.advisor.interactions;

import java.sql.SQLException;
import java.util.List;
import java.util.Set;

import static java.lang.Math.abs;
import static java.lang.Math.max;

import com.google.common.collect.Sets;

import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.optimizer.PreparedSQLStatement;

import static com.google.common.collect.Sets.cartesianProduct;
import static com.google.common.collect.Sets.difference;
import static com.google.common.collect.Sets.powerSet;
import static com.google.common.collect.Sets.union;

/**
 * Computes the degree of interaction by implementing the NAIVE algorithm described in "Index 
 * interactions in physical design tuning: modeling, analysis, and applications", but instead of 
 * operating on an IBG, it assumes that the given is an implementation of a {@link 
 * DefaultPreparedSQLStatement}.
 * <p>
 * This implementation can be thought as of using an {@link Optimizer} as a black box. This is in 
 * contrast to more sophisticated techniques, like {@link IBGDoiFinder}, which may explore the 
 * internal structure of a {@link PreparedSQLStatement} in order to improve the performance of the 
 * interaction discovering process.
 *
 * @author Ivo Jimenez
 * @see <a href="http://portal.acm.org/citation.cfm?id=1687766">
 *         Index interactions in physical design tuning: modeling, analysis, and applications
 *      </a>
 */
public class DefaultDoiFinder implements DegreeOfInteractionFinder
{
    /**
     * {@inheritDoc}
     */
    public InteractionBank degreeOfInteraction(PreparedSQLStatement sql, Set<Index> s)
        throws SQLException
    {
        InteractionBank bank = new InteractionBank(s);

        for (Set<Index> xAll : powerSet(s)) {

            Set<Index> x = difference(s, xAll).immutableCopy();

            @SuppressWarnings("unchecked")
            Set<List<Index>> pairs = cartesianProduct(x, x);

            for (List<Index> pair : pairs) {

                final Index a = pair.get(0);
                final Index b = pair.get(1);

                if (a.equals(b))
                    continue;

                final Set<Index> xa = union(x, Sets.<Index>newHashSet(a)).immutableCopy();
                final Set<Index> xb = union(x, Sets.<Index>newHashSet(b)).immutableCopy();
                final Set<Index> xab = union(xa, Sets.<Index>newHashSet(b)).immutableCopy();

                final double costX = sql.explain(x).getSelectCost();
                final double costXa = sql.explain(xa).getSelectCost();
                final double costXb = sql.explain(xb).getSelectCost();
                final double costXab = sql.explain(xab).getSelectCost();

                final double d = abs(costX - costXa - costXb + costXab) / costXab;

                bank.assignInteraction(a, b, max(bank.interactionLevel(a, b), d));
            }
        }

        return bank;
    }
}
