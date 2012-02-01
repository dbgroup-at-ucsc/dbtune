package edu.ucsc.dbtune.inum;

import java.sql.SQLException;

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
}