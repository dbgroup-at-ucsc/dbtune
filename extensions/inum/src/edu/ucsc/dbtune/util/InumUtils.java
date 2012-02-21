package edu.ucsc.dbtune.util;

import java.sql.SQLException;
import java.util.Set;

import edu.ucsc.dbtune.inum.FullTableScanIndex;
import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.metadata.Table;
import edu.ucsc.dbtune.optimizer.plan.InumPlan;
import edu.ucsc.dbtune.optimizer.plan.SQLStatementPlan;

import static com.google.common.collect.Iterables.get;

import static edu.ucsc.dbtune.util.MetadataUtils.getIndexesReferencingTables;

/**
 * @author Ivo Jimenez
 */
public final class InumUtils
{
    /**
     * utility class.
     */
    private InumUtils()
    {
    }

    /**
     * Completes a given configuration by adding the FTS index. A complete configuration is 
     * guaranteed to have at least one index for every table referenced in the INUM space statement, 
     * where the worst case is when the {@link FullTableScanIndex} is the only index for a 
     * particular table.
     *
     * @param inumSpace
     *      the set of template plans
     * @param configuration
     *      set of indexes that will be completed by adding FTS for each table referenced by the 
     *      inum space
     * @return
     *      a complete configuration
     * @throws SQLException
     *      if {@link #getFullTableScanIndexInstance} throws an exception
     */
    public static Set<Index> complete(Set<InumPlan> inumSpace, Set<Index> configuration)
        throws SQLException
    {
        return complete(get(inumSpace, 0), configuration);
    }

    /**
     * Completes a given configuration by adding the FTS index. A complete configuration is 
     * guaranteed to have at least one index for every table referenced in the INUM space statement, 
     * where the worst case is when the {@link FullTableScanIndex} is the only index for a 
     * particular table.
     *
     * @param plan
     *      a representative plan of the INUM space
     * @param configuration
     *      set of indexes that will be completed by adding FTS for each table referenced by the 
     *      inum space
     * @return
     *      a complete configuration
     * @throws SQLException
     *      if {@link #getFullTableScanIndexInstance} throws an exception
     */
    public static Set<Index> complete(SQLStatementPlan plan, Set<Index> configuration)
        throws SQLException
    {
        Set<Index> indexes = getIndexesReferencingTables(configuration, plan.getTables());

        for (Table table : plan.getTables())
            indexes.add(FullTableScanIndex.getFullTableScanIndexInstance(table));

        return indexes;
    }
}
