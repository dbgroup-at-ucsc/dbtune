package edu.ucsc.dbtune.util;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.metadata.Table;

/**
 * @author Ivo Jimenez
 */
public final class MetadataUtils
{
    /**
     * Utility class.
     */
    private MetadataUtils()
    {
    }

    /**
     * Returns the set of tables referenced by the given collection of indexes.
     *
     * @param indexes
     *      a collection of indexes
     * @return
     *      the set of tables corresponding to one or more indexes in the set
     */
    public static Set<Table> getTables(Collection<Index> indexes)
    {
        Set<Table> tables = new HashSet<Table>();

        for (Index i : indexes)
            tables.add(i.getTable());

        return tables;
    }
}
