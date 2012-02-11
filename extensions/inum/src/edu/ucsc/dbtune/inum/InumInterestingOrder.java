package edu.ucsc.dbtune.inum;

import java.sql.SQLException;

import java.util.List;
import java.util.Map;

import edu.ucsc.dbtune.metadata.Column;
import edu.ucsc.dbtune.metadata.Schema;
import edu.ucsc.dbtune.metadata.Table;

import edu.ucsc.dbtune.optimizer.plan.InterestingOrder;

/**
 * Represents an interesting order in the Inum package. This is only defined to allow the easy 
 * identification of interesting orders created by the INUM package, as opposed to {@link 
 * InterestingOrder} instances created by the core API.
 *
 * @author Ivo Jimenez
 */
public class InumInterestingOrder extends InterestingOrder
{
    /**
     * Invoked only by {@link FullTableScanIndex}.
     *
     * @param schema
     *     schema that corresponds to the index
     * @param table
     *     table where the index is defined in
     * @param indexName
     *     name of the index
     * @throws SQLException
     *     if the schema of the table is null or can't be retrieved
     */
    InumInterestingOrder(Schema schema, Table table, String indexName) throws SQLException
    {
        super(table.columns().get(0), ASCENDING);

        this.name = indexName;
        this.table = table;
        this.remove(table.columns().get(0));

        container.remove(this);
    }

    /**
     * Creates an interesting order instance.
     *
     * @param column
     *     column that will define the index
     * @param ascending
     *     indicates whether or not the column is sorted in ascending or ascending order.
     * @throws SQLException
     *      if the schema of the table is null or can't be retrieved
     */
    public InumInterestingOrder(Column column, boolean ascending) throws SQLException
    {
        super(column, ascending);
    }

    /**
     * Creates an index containing the given columns and ascending values. The name of the index is 
     * defaulted to {@code "dbtune_" + getId() + "_index"}. The index is assumed to be {@link 
     * SECONDARY},  {@link NON_UNIQUE} and {@link UNCLUSTERED}
     *
     * @param columns
     *     columns that will define the index
     * @param ascending
     *     indicates whether or not the corresponding column is sorted in ascending or ascending 
     *     order.
     * @throws SQLException
     *     if column list empty; if schema already contains an index with the defaulted name; if not 
     *     all of the columns in the list correspond to the same table.
     */
    public InumInterestingOrder(List<Column> columns, Map<Column, Boolean> ascending)
        throws SQLException
    {
        super(columns, ascending);
    }
    
    public InumInterestingOrder(InterestingOrder io) throws SQLException
    {
        super(io.columns(),io.getAscending());
    }
}
