package edu.ucsc.dbtune.inum;

import java.sql.SQLException;
import java.util.Set;

import edu.ucsc.dbtune.metadata.ColumnOrdering;

import edu.ucsc.dbtune.workload.SQLStatement;

/**
 * Extracts interesting orders out of SQL statements.
 *
 * @author Huascar A. Sanchez
 * @author Ivo Jimenez
 */
public interface InterestingOrdersExtractor
{
    /**
     * Extracts the interesting orders for a given statement.
     *
     * @param statement
     *      statement from which the set of interesting orders get extracted
     * @return
     *      a mapping of tables to sets of interesting orders, one per every table referenced in the 
     *      statement
     * @throws SQLException
     *      if the statement doesn't imply any interesting order. For example, if the statement 
     *      refers only one table in the {@code FROM} clause and it doesn't contain neither an 
     *      {@code ORDER BY} nor a {@code GROUP BY} clause.
     */
    Set<ColumnOrdering> extract(SQLStatement statement) throws SQLException;
}
