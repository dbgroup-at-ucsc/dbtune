package edu.ucsc.dbtune.inum;

import java.sql.SQLException;

import java.util.List;
import java.util.Map;

import edu.ucsc.dbtune.metadata.Column;
import edu.ucsc.dbtune.metadata.Index;
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
public class InumInterestingOrder extends Index
{
    protected Table table;

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
        this(table.columns().get(0), ASCENDING);

        this.name = indexName;
        this.table = table;
        this.remove(table.columns().get(0));

        container.remove(this);
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
    public InumInterestingOrder(List<Column> columns, List<Boolean> ascending) throws SQLException
    {
        super("temporary", columns, ascending);

        container.remove(this);

        // we know the list has at least one column; it's a precondition of the super constructor
        table = columns.get(0).getTable();

        // override name
        name = makeName(columns, ascending);
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
    public InumInterestingOrder(List<Column> columns, Map<Column, Boolean> ascending)
        throws SQLException
    {
        super("temporary", columns, ascending);

        container.remove(this);

        // we know the list has at least one column; it's a precondition of the super constructor
        table = columns.get(0).getTable();

        // override name
        name = makeName(columns, ascending);
    }

    /**
     * copy constructor.
     *
     * @param io
     *      other object
     * @throws SQLException
     *      if error
     */
    public InumInterestingOrder(InumInterestingOrder io) throws SQLException
    {
        this(((Index) io).columns(), io.ascendingColumn);
    }

    /**
     * copy constructor.
     *
     * @param io
     *      other object
     * @throws SQLException
     *      if error
     */
    public InumInterestingOrder(InterestingOrder io) throws SQLException
    {
        this(((Index) io).columns(), io.getAscending());
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
        super(column.getTable().getSchema(), makeName(column, ascending));

        add(column, ascending);

        table = column.getTable();

        container.remove(this);
    }


    /**
     * {@inheritDoc}
     */
    @Override
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
        return column.getName() + "_" + (ascending ? "a" : "d") + "_";
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

        for (Column c : columns)
            str.append(makeName(c, ascending.get(c)));

        return str.toString();
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
    private static String makeName(List<Column> columns, List<Boolean> ascending)
    {
        StringBuilder str = new StringBuilder();

        for (int i = 0; i < columns.size(); i++)
            str.append(makeName(columns.get(i), ascending.get(i)));

        return str.toString();
    }
}
