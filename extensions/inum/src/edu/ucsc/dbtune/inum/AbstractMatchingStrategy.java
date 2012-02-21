package edu.ucsc.dbtune.inum;

import java.sql.SQLException;
import java.util.Set;

import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.optimizer.plan.InumPlan;

import static edu.ucsc.dbtune.util.InumUtils.complete;

/**
 * Base matching strategy. Implementors of this class should only implement the {@link #matchAtomic} 
 * method. This abstract implementation ensures that all configurations passed to the {@link 
 * #matchCompleteConfiguration} method reference at least one table by inserting the {@link 
 * FullTableScanIndex} singleton instance for every table.
 *
 * @author Ivo Jimenez
 */
public abstract class AbstractMatchingStrategy implements MatchingStrategy
{
    /**
     * {@inheritDoc}
     */
    @Override
    public final Result match(Set<InumPlan> inumSpace, Set<Index> configuration)
        throws SQLException
    {
        return matchCompleteConfiguration(inumSpace, complete(inumSpace, configuration));
    }

    /**
     * Matches a complete configuration. A complete configuration is guaranteed to have at least one 
     * index for every table referenced in the INUM space statement, where the worst case is when 
     * the {@link FullTableScanIndex} is the only index for a particular table.
     *
     * @param inumSpace
     *      plans in the INUM space
     * @param configuration
     *      a configuration that has at least one index per table
     * @return
     *      result of the matching
     * @throws SQLException
     *      if an error occurs while doing the matching
     */
    public abstract Result matchCompleteConfiguration(
            Set<InumPlan> inumSpace, Set<Index> configuration)
        throws SQLException;
}
