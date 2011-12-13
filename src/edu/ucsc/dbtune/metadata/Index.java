package edu.ucsc.dbtune.metadata;

import edu.ucsc.dbtune.util.Objects;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import java.sql.SQLException;

/**
 * Represents the abstraction for Index metadata
 *
 * @author Ivo Jimenez
 */
public class Index extends DatabaseObject implements Iterable<Column>
{
    public static final int     UNKNOWN        = 0;
    public static final int     B_TREE         = 1;
    public static final int     BITMAP         = 2;
    public static final int     HASH           = 3;
    public static final int     REVERSIBLE     = 4;
    public static final int     NON_REVERSIBLE = 5;
    public static final int     SYNCHRONIZED   = 6;
    public static final boolean PRIMARY        = true;
    public static final boolean SECONDARY      = false;
    public static final boolean CLUSTERED      = true;
    public static final boolean UNCLUSTERED    = false;
    public static final boolean UNIQUE         = true;
    public static final boolean NON_UNIQUE     = false;
    public static final boolean ASCENDING      = true;
    public static final boolean DESCENDING     = false;

    protected List<Boolean> descending;
    protected int           type;
    protected int           scanOption;
    protected boolean       unique;
    protected boolean       primary;
    protected boolean       clustered;
    protected boolean       materialized;

    /**
     * Creates an empty index. The index is assumed to be {@link SECONDARY}, {@link NON_UNIQUE} and 
     * {@link UNCLUSTERED}
     *
     * @param table
     *     schema where the index will be contained.
     * @param name
     *     name of the index
     * @throws SQLException
     *     if schema already contains an index with the defaulted name
     */
    public Index(Schema schema, String name) throws SQLException
    {
        this(schema,name,new ArrayList<Column>(), new ArrayList<Boolean>(),SECONDARY,NON_UNIQUE,UNCLUSTERED);
    }

    /**
     * Creates an empty index.
     *
     * @param schema
     *     schema where the index will be contained.
     * @param name
     *     name of the index
     * @param primary
     *     whether or not the index is primary
     * @param unique
     *     whether the index is unique or not.
     * @param clustered
     *     whether the corresponding table is clustered on this index
     * @throws SQLException
     *     if schema already contains an index with the defaulted name
     */
    public Index(
            Schema schema, 
            String name, 
            boolean primary,
            boolean unique, 
            boolean clustered)
        throws SQLException
    {
        this(schema,name,new ArrayList<Column>(), new ArrayList<Boolean>(),primary,unique,clustered);
    }

    /**
     * Creates an index containing the given column. The name of the index is defaulted to {@code 
     * column.getName()+"_index"}. The column is taken as being in descending order. The index is 
     * assumed to be {@link SECONDARY}, {@link NON_UNIQUE} and {@link UNCLUSTERED}
     *
     * @param column
     *     column that will define the index
     * @throws SQLException
     *     if column list empty; if schema already contains an index with the defaulted name; if not 
     *     all of the columns in the list correspond to the same table.
     */
    public Index(Column column)
        throws SQLException
    {
        this(column, SECONDARY, NON_UNIQUE, UNCLUSTERED);
    }

    /**
     * Creates an index containing the given column. The column is taken as being in descending 
     * order. The index is assumed to be {@link SECONDARY}, {@link NON_UNIQUE} and {@link 
     * UNCLUSTERED}
     *
     * @param column
     *     column that will define the index
     * @throws SQLException
     *     if column list empty; if schema already contains an index with the defaulted name; if not 
     *     all of the columns in the list correspond to the same table.
     */
    public Index(String name, Column column)
        throws SQLException
    {
        this(name, column, SECONDARY, NON_UNIQUE, UNCLUSTERED);
    }

    /**
     * Creates an index with the given column, primary, uniqueness and clustering values. The name 
     * of the index is defaulted to {@code column.getName()+"_index"}. The column is taken as being 
     * in descending order.
     *
     * @param column
     *     column that will define the index
     * @param primary
     *     whether or not the index is primary
     * @param unique
     *     whether the index is unique or not.
     * @param clustered
     *     whether the corresponding table is clustered on this index
     * @throws SQLException
     *     if column list empty; if schema already contains an index with the defaulted name; if not 
     *     all of the columns in the list correspond to the same table.
     */
    public Index(Column column, boolean primary, boolean unique, boolean clustered)
        throws SQLException
    {
        this(column.getName()+"_index", column, primary, unique, clustered);
    }

    /**
     * Creates an index with the given name, column, primary, uniqueness and clustering values. The 
     * column is taken as being in ascending order.
     *
     * @param name
     *     name of the index
     * @param column
     *     column that will define the index
     * @param primary
     *     whether or not the index is primary
     * @param unique
     *     whether the index is unique or not.
     * @param clustered
     *     whether the corresponding table is clustered on this index
     * @throws SQLException
     *     if column list empty; if schema already contains an index with the defaulted name; if not 
     *     all of the columns in the list correspond to the same table.
     */
    public Index(String name, Column column, boolean primary, boolean unique, boolean clustered)
        throws SQLException
    {
        this((Schema)column.getContainer().getContainer(), name, primary, unique, clustered);
        add(column);
        descending.add(ASCENDING);
    }

    /**
     * Creates an index from the given columns, primary, uniqueness and clustering values. Columns
     * are taken as ordered in ascending order.
     *
     * @param name
     *     name of the index
     * @param columns
     *     columns that will define the index
     * @param primary
     *     whether or not the index is primary
     * @param clustered
     *     whether the corresponding table is clustered on this index
     * @throws SQLException
     *     if column list empty; if schema already contains an index with the given name; if not all 
     *     of the columns in the list correspond to the same table.
     */
    public Index(String name, List<Column> columns, boolean primary, boolean unique, boolean clustered)
        throws SQLException
    {
        this(null,name,columns,null,primary,unique,clustered);
    }
    /**
     * Creates an index from the given columns, primary, uniqueness and clustering values.
     *
     * @param name
     *     name of the index
     * @param columns
     *     columns that will define the index
     * @param descending
     *     indicates whether or not the corresponding column is sorted in ascending or descending 
     *     order.
     * @param primary
     *     whether or not the index is primary
     * @param clustered
     *     whether the corresponding table is clustered on this index
     * @throws SQLException
     *     if column list empty; if schema already contains an index with the given name; if not all 
     *     of the columns in the list correspond to the same table.
     */
    public Index(
            String name,
            List<Column> columns,
            List<Boolean> descending,
            boolean primary,
            boolean unique,
            boolean clustered)
        throws SQLException
    {
        this(null,name,columns,descending,primary,unique,clustered);
    }

    /**
     * Creates an index from the given columns, primary, uniqueness and clustering values.
     *
     * @param schema
     *     schema where the index will be contained.
     * @param name
     *     name of the index
     * @param columns
     *     columns that will define the index
     * @param descending
     *     indicates whether or not the corresponding column is sorted in ascending or descending 
     *     order.
     * @param primary
     *     whether or not the index is primary
     * @param clustered
     *     whether the corresponding table is clustered on this index
     * @throws SQLException
     *     if column list empty; if schema already contains an index with the given name; if not all 
     *     of the columns in the list correspond to the same table.
     */
    Index(
            Schema schema,
            String name,
            List<Column> columns,
            List<Boolean> descending,
            boolean primary,
            boolean unique,
            boolean clustered)
        throws SQLException
    {
        super(name);

        if (schema == null && columns.size() == 0)
            throw new SQLException("Column list should have at least one element");

        Table table = null;

        if (schema == null) {
            table     = (Table) columns.get(0).container;
            schema    = (Schema) table.container;
            container = schema;
        }

        this.descending = new ArrayList<Boolean>();

        if (descending == null)
            for (int i = 0; i < columns.size(); i++)
                this.descending.add(ASCENDING);
        else if (descending.size() != columns.size())
            throw new SQLException("Incorrect number of descending values");
        else
            this.descending = new ArrayList<Boolean>(descending);

        for (int i = 0; i < columns.size(); i++) {
            if (table == null)
                table = (Table) columns.get(i).container;

            if (table != columns.get(i).container)
                throw new SQLException("Columns from different tables");

            add(columns.get(i));
        }

        this.type       = UNKNOWN;
        this.primary    = primary;
        this.unique     = unique;
        this.clustered  = clustered;
        this.scanOption = NON_REVERSIBLE;
        this.container  = schema;

        container.add(this);
    }

    /**
     * Copy constructor
     *
     * @param other
     *     other index being copied into the new one
     */
    protected Index(Index other)
    {
        super(other);

        this.type         = other.type;
        this.unique       = other.unique;
        this.primary      = other.primary;
        this.clustered    = other.clustered;
        this.materialized = other.materialized;
        this.descending   = other.descending;
        this.scanOption   = other.scanOption;
    }

    /**
     * Sets the type of index. Either PRIMARY, CLUSTERED or SECONDARY.
     *
     * @param type
     *     one of the available fields.
     */
    public void setType(int type)
    {
        switch(type)
        {
            case UNKNOWN:
            case B_TREE:
            case BITMAP:
                break;
            default:
                throw new RuntimeException("Invalid type " + type);
        }

        this.type = type;
    }

    /**
     * Returns the type of the index.
     *
     * @return
     *     type of this index
     */
    public int getType()
    {
        return type;
    }

    /**
     * Sets the value of the <code>unique</code> property of this index.
     *
     * @param unique
     *     value to be assigned
     */
    public void setUnique(boolean unique)
    {
        this.unique = unique;
    }

    /**
     * Returns the scan option of the index.
     *
     * @return
     *     scan option of this index
     */
    public int getScanOption()
    {
        return scanOption;
    }

    /**
     * Sets the value of the <code>scanOption</code> property of this index.
     *
     * @param scanOption
     *     value to be assigned
     */
    public void setScanOption(int scanOption)
    {
        this.scanOption = scanOption;
    }

    /**
     * Whether or not this index is unique.
     *
     * @return
     *     <code>true</code> if unique; <code>false</code> otherwise
     */
    public boolean isUnique()
    {
        return unique;
    }

    /**
     * Sets the value of the <code>primary</code> property of this index.
     *
     * @param primary
     *     value to be assigned
     */
    public void setPrimary(boolean primary)
    {
        this.primary = primary;
    }

    /**
     * Whether or not this index corresponds to the table's primary key.
     *
     * @return
     *     <code>true</code> if table's primary key contains the same columns as this index; 
     *     <code>false</code> otherwise
     */
    public boolean isPrimary()
    {
        return primary;
    }

    /**
     * Sets the value of the <code>clustered</code> property of this index.
     *
     * @param clustered
     *     whether or not the index is clustered
     */
    public void setClustered(boolean clustered)
    {
        this.clustered = clustered;
    }

    /**
     * Whether or not this index defines the corresponding's table clustering. For indexes that are 
     * materialized, only one of them can return <code>true</code>.
     *
     * @return
     *     <code>true</code> if table's clustered on it; <code>false</code> otherwise
     */
    public boolean isClustered()
    {
        return clustered;
    }

    /**
     * Sets the value of the <code>materialized</code> property of this index.
     *
     * @param materialized
     *     value to be assigned
     */
    public void setMaterialized(boolean materialized)
    {
        this.materialized = materialized;
    }

    /**
     * Whether or not this index is materialized, i.e. exists in the database and not only as an 
     * in-memory object.
     *
     * @return
     *     <code>true</code> if index is materialized; <code>false</code> otherwise
     */
    public boolean isMaterialized()
    {
        return materialized;
    }

    /**
     * Returns the schema that contains this index. Convenience method that accomplishes what {@link 
     * #getContainer} does but without requiring the user to cast. In other words, the following is 
     * true {@code getSchema() == (Schema) getContainer()}.
     *
     * @return
     *     the schema that contains this object
     */
    public Schema getSchema()
    {
        return (Schema) container;
    }

    /**
     * Returns the table on which the index is defined.
     *
     * @return
     *     the table that this index refers to.
     */
    public Table getTable()
    {
        if (containees.size() > 0)
            return (Table) containees.get(0).getContainer();
        else
            throw new RuntimeException("No columns on index");
    }

    /**
     * adds a new column to the index with the given order. If the column is already contained it 
     * does nothing, i.e. repetitions aren't allowed
     *
     * @param column
     *     new column to be inserted to the sequence
     * @param descending
     *     {@code true} if the column is in descending order. {@code false} if ascending.
     * @throws SQLException
     *     if column is already contained in the index
     */
    public void add(Column column, Boolean descending) throws SQLException
    {
        if (containees.size() != 0 && containees.get(0).getContainer() != column.container)
            throw new SQLException("Table " + column.getContainer().getName() +
                                   " doesn't correspond to " + containees.get(0).getContainer());

        super.add(column);
        this.descending.add(descending);
    }

    /**
     * adds a new column to the index in ascending order. If the column is already contained it does 
     * nothing, i.e.  repetitions aren't allowed
     *
     * @param column
     *     new column to be inserted to the sequence
     * @throws SQLException
     *     if column is already contained in the index
     */
    public void add(Column column) throws SQLException
    {
        add(column,ASCENDING);
    }

    /**
     * Returns the descending value for the given column
     *
     * @return
     *     <code>true</code> the column is descending; <code>false</code> if ascending
     * @throws SQLException
     *     if the column isn't contained in the index
     */
    public boolean isDescending(Column column) throws SQLException
    {
        try {
            return descending.get(containees.indexOf(column));
        } catch (IndexOutOfBoundsException ex) {
            throw new SQLException(ex);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public DatabaseObject newContainee(String name) throws SQLException
    {
        throw new SQLException("Can't instantiate columns like this");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean equalsContent(Object other)
    {
        if (!(other instanceof Index))
            return false;

        Index idx = (Index) other;

        if (containees.size() != idx.containees.size())
            return false;

        for (int i = 0; i < idx.size(); i++) {
            if (containees.get(i) != idx.containees.get(i))
                return false;

            if (descending.get(i) != idx.descending.get(i))
                return false;
        }

        if (type != idx.type ||
                unique != idx.unique ||
                primary != idx.primary ||
                clustered != idx.clustered ||
                materialized != idx.materialized ||
                scanOption != idx.scanOption)
            return false;

        return true;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Iterator<Column> iterator()
    {
        return Objects.<Iterator<Column>>as(containees.iterator());
    }

    /**
     * {@inheritDoc}
     */
    public Iterable<Column> columns()
    {
        return Objects.<Iterable<Column>>as(containees);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isValid(DatabaseObject dbo)
    {
        return dbo instanceof Column;
    }
}
