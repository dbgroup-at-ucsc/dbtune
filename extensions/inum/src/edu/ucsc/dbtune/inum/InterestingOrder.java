package edu.ucsc.dbtune.inum;

import java.sql.SQLException;

import java.util.List;
import java.util.Map;

import edu.ucsc.dbtune.metadata.Column;
import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.metadata.Schema;
import edu.ucsc.dbtune.metadata.Table;

/**
 * Represents an interesting order. This class descends from {@link Index} but only for the purposes 
 * of reusing the comparison logic, that is, the containment logic that is implemented in {@link 
 * edu.ucsc.dbtune.metadata.DatabaseObject} is not used here and thus it shouldn't be invoked from 
 * this or the descendants of this class, otherwise the container-based operations won't count this 
 * object as being contained in an object and will throw exceptions.
 * <p>
 * It is important that this derivation from Index is maintained since the {@link 
 * edu.ucsc.optimizer} package relies on {@link Index} for doing what-if optimization.
 *
 * @author Ivo Jimenez
 */
class InterestingOrder extends Index
{
    private Table table;

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
    InterestingOrder(Schema schema, Table table, String indexName) throws SQLException
    {
        super(schema, indexName);

        this.table = table;

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
    public InterestingOrder(Column column, boolean ascending) throws SQLException
    {
        super(column.getTable().getSchema(), makeName(column, ascending));

        container.remove(this);

        add(column);

        table = column.getTable();
    }

    /**
     * Creates an interesting order instance.
     *
     * @param columns
     *     column that will define the index
     * @param ascending
     *     whether or not the column is sorted in ascending or ascending order.
     * @throws SQLException
     *      if the schema of the table is null or can't be retrieved
     */
    public InterestingOrder(List<Column> columns, Map<Column, Boolean> ascending)
        throws SQLException
    {
        super("temporary", columns, ascending);

        container.remove(this);

        // we know container has at least one column; is a precondition in super constructor
        table = columns.get(0).getTable();

        // override name
        name = makeName(columns, ascending);
    }

    /**
     * {@inheritDoc}
     */
    public Table getTable()
    {
        return this.table;
    }

    /**
     * Creates the name of an index.
     *
     * @param column
     *     column that will define the index
     * @param ascending
     *     indicates whether or not the column is sorted in ascending or ascending order.
     * @return
     *      then name of the index
     */
    private static String makeName(Column column, boolean ascending)
    {
        return column.getName() + "_" + (ascending ? "a" : "d");
    }

    /**
     * Creates the name of an index.
     *
     * @param columns
     *      column that will define the index
     * @param ascending
     *      whether or not the column is sorted in ascending or ascending order.
     * @return
     *      then name of the index
     */
    private static String makeName(List<Column> columns, Map<Column, Boolean> ascending)
    {
        StringBuilder str = new StringBuilder();

        for (Column c : columns) {
            str.append(makeName(c, ascending.get(c))).append("_");
        }

        return str.toString();
    }
}
