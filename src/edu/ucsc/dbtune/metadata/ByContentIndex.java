package edu.ucsc.dbtune.metadata;

import java.sql.SQLException;

/**
 * An index that is identified by its content instead of by fully qualified name.
 *
 * @author Ivo Jimenez
 */
public class ByContentIndex extends Index
{
    /**
     * Default value
     */
    private static final long serialVersionUID = 1L;

    /**
     * Creates an index containing the given column. The name of the index is defaulted to {@code 
     * table + "_" + columnName + "_" + ascending + "_index"}. The index is assumed to be {@link 
     * SECONDARY},  {@link NON_UNIQUE} and {@link UNCLUSTERED}.
     *
     * @param column
     *     column that will define the index
     * @param ascending
     *     indicates whether or not the corresponding column is sorted in ascending or ascending 
     *     order.
     * @throws SQLException
     *     if column list empty; if schema already contains an index with the defaulted name; if not 
     *     all of the columns in the list correspond to the same table.
     */
    public ByContentIndex(Column column, boolean ascending) throws SQLException
    {
        super(column.getTable().getName() + "_" + column.getName() + "_" + ascending + "_index", 
                column, ascending, SECONDARY, NON_UNIQUE, UNCLUSTERED);
    }

    /**
     * Copy constructor.
     *
     * @param other
     *     other index being copied into the new one
     */
    public ByContentIndex(Index other)
    {
        super(other);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean equals(Object other)
    {
        if (!(other instanceof Index))
            return false;

        Index o = (Index) other;

        return this.equalsContent(o);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int hashCode()
    {
        int result = 0;

        result = 37 * result + size();
        result = 37 * result + containees.hashCode();
        result = 37 * result + ascendingColumn.hashCode();
        result = 37 * result + type;
        result = 37 * result + (unique ? 0 : 1);
        result = 37 * result + (primary ? 0 : 1);
        result = 37 * result + (clustered ? 0 : 1);
        result = 37 * result + (materialized ? 0 : 1);
        result = 37 * result + scanOption;

        return result;
    }
}
