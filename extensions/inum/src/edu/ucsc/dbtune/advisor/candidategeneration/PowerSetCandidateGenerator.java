package edu.ucsc.dbtune.advisor.candidategeneration;

import java.sql.SQLException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import edu.ucsc.dbtune.inum.DerbyReferencedColumnsExtractor;
import edu.ucsc.dbtune.metadata.Catalog;
import edu.ucsc.dbtune.metadata.Column;
import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.metadata.Table;
import edu.ucsc.dbtune.util.Sets;
import edu.ucsc.dbtune.workload.SQLStatement;

import org.apache.derby.iapi.error.StandardException;

/**
 * @author Quoc Trung Tran
 */
public class PowerSetCandidateGenerator extends AbstractCandidateGenerator
{
    private DerbyReferencedColumnsExtractor extractor;
    private int maxCols;
    
    /**
     * Construct a generator with a given catalog and a default ascending order of the column
     * in the derived indexes.
     * 
     * @param catalog
     *      The catalog to bind columns in the queries to database objects
     * @param maxCols
     *      The maximum number of columns in each index     
     * @param defaultAscending
     *      The default ascending order of each column in the indexes
     */
    public PowerSetCandidateGenerator(Catalog catalog, int maxCols, boolean defaultAscending)
    {
        this.maxCols = maxCols;
        extractor = new DerbyReferencedColumnsExtractor(catalog, defaultAscending);
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public Set<Index> generate(SQLStatement sql) throws SQLException
    {
        Map<Column, Boolean> ascending = new HashMap<Column, Boolean>();

        try {
            ascending.putAll(extractor.getReferencedColumn(sql));
        } catch (StandardException e) {
            throw new SQLException("An error occurred while iterating the from list", e);
        }

        Set<Column> columns = ascending.keySet();

        // Extract columns that belong to the same relation
        Map<Table, Set<Column>> columnsPerTable = new HashMap<Table, Set<Column>>();

        for (Column col : columns) {
            Table table = col.getTable();
            Set<Column> set = columnsPerTable.get(table);
            if (set == null) {
                set = new HashSet<Column>();
            }

            set.add(col);
            columnsPerTable.put(table, set);
        }

        // enumerate powerset for each set of columns belonging to the same relation

        Sets<Column> sets = new Sets<Column>();
        Set<Index> indexes = new HashSet<Index>(); 

        for (Set<Column> setColumnPerTable : columnsPerTable.values())
            for (int k = 1; k <= maxCols; k++)
                for (Set<Column> colIndex : sets.powerSet(setColumnPerTable, k)) {

                    Map<Column, Boolean> ascendingIndex = new HashMap<Column, Boolean>();

                    for (Column col : colIndex)
                        ascendingIndex.put(col, ascending.get(col));

                    indexes.add(new Index(new ArrayList<Column>(colIndex), ascendingIndex));
                }

        return indexes;
    }
}
