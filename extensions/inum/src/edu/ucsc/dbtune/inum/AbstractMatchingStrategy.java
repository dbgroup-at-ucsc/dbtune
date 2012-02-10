package edu.ucsc.dbtune.inum;

import java.sql.SQLException;

import java.util.List;
import java.util.Set;

import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.metadata.Table;
import edu.ucsc.dbtune.optimizer.plan.InumPlan;

import static com.google.common.collect.Iterables.get;
import static com.google.common.collect.Sets.difference;
import static com.google.common.collect.Sets.newHashSet;

import static edu.ucsc.dbtune.util.MetadataUtils.getIndexesPerTable;
import static edu.ucsc.dbtune.util.MetadataUtils.getIndexesReferencingTables;
import static edu.ucsc.dbtune.util.MetadataUtils.getReferencedTables;

/**
 * Base matching strategy. Implementors of this class should only implement the {@link #matchAtomic} 
 * method. This abstract implementation ensures that all configurations passed to the {@link 
 * #matchCompleteConfiguration} method reference at least one table by inserting the {@link 
 * FullTableScanIndex} singleton instance for every table that is not referenced by the given 
 * configuration.
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
        Set<Index> indexes = newHashSet(configuration);

        List<Table> tablesReferencedInStmt = get(inumSpace, 0).getTables();

        if (tablesReferencedInStmt.size() != getIndexesPerTable(indexes).keySet().size())
            addFullTableScanIndexForMissingTables(tablesReferencedInStmt, indexes);

        Set<Index> indexesReferencingTables =
            getIndexesReferencingTables(indexes, tablesReferencedInStmt);

        return matchCompleteConfiguration(inumSpace, indexesReferencingTables);
    }

    /**
     * Matches a complete configuration. A complete configuration is guaranteed to have at least one 
     * index for every table referenced in the INUM space statement.
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

    /**
     * Adds {@link FullTableScanIndex} instances for each table contained in {@code tables} that is 
     * not referenced by indexes in {@code indexes}.
     *
     * @param tablesReferencedInStmt
     *      all the tables that should be referenced by the set of indexes in
     * @param indexes
     *      indexes that get inspected
     * @throws SQLException
     *      if {@link FullTableScanIndex#getFullTableScanIndexInstance} throws an exception
     */
    private static void addFullTableScanIndexForMissingTables(
            List<Table> tablesReferencedInStmt, Set<Index> indexes)
        throws SQLException
    {
        Set<Table> tablesNotInConfiguration =
            difference(newHashSet(tablesReferencedInStmt), getReferencedTables(indexes));
        
        for (Table table : tablesNotInConfiguration)
            indexes.add(FullTableScanIndex.getFullTableScanIndexInstance(table));
    }
}
