package edu.ucsc.dbtune.optimizer.plan;

import java.sql.SQLException;

import java.util.List;
import java.util.Map;

import edu.ucsc.dbtune.metadata.Column;
import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.metadata.Table;

/**
 * Represents an interesting order. This class descends from {@link Index} but only for the purposes 
 * of reusing the comparison logic, specifically, the methods {@link #getFullyQualifiedName}, {@link 
 * #hashCode}, {@link #toString}, {@link #equals}. The containment logic that is implemented in 
 * {@link edu.ucsc.dbtune.metadata.DatabaseObject} is not used here and thus it shouldn't be invoked 
 * from this or the descendants of this class, otherwise the container-based operations (like {@link 
 * #contains}, {@link #size}, {@link #getAll}, {@link #getId}, etc.) of the corresponding container 
 * (i.e. a schema) won't count this object as being contained in an object and will throw 
 * exceptions.
 * <p>
 * It is important that this derivation from {@link Index} is maintained since the {@link 
 * edu.ucsc.optimizer} package relies on {@link Index} for doing what-if optimization.
 *
 * @author Ivo Jimenez
 */
public class InterestingOrder extends Index
{
    protected Table table;

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

        add(column, ascending);

        table = column.getTable();

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
    public InterestingOrder(List<Column> columns, Map<Column, Boolean> ascending)
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
}
