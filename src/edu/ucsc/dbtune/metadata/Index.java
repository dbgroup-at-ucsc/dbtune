package edu.ucsc.dbtune.metadata;


import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import edu.ucsc.dbtune.util.IncrementallyIdentifiable;
import edu.ucsc.dbtune.util.Objects;

/**
 * Represents the abstraction for Index metadata.
 *
 * @author Ivo Jimenez
 */
public class Index extends DatabaseObject implements 
                                Iterable<Column>, IncrementallyIdentifiable
{
    /**
     * Default the number
     */
    private static final long serialVersionUID = 1L;
    
    
    // CHECKSTYLE:OFF
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
    public static final boolean ASC            = true;
    public static final boolean DESCENDING     = false;
    public static final boolean DESC           = false;

    /** used to uniquely identify each instance of the class. */
    public static AtomicInteger IN_MEMORY_ID = new AtomicInteger(1);
    // CHECKSTYLE:ON

    protected List<Boolean> ascendingColumn;
    protected int           type;
    protected int           scanOption;
    protected int           inMemoryID;
    protected boolean       unique;
    protected boolean       primary;
    protected boolean       clustered;
    protected boolean       materialized;

    /**
     * Creates an index containing the given column. The name of the index is defaulted to {@code 
     * "dbtune_" + getId() + "_index"}. The column is taken as being in ascending order. The index 
     * is assumed to be {@link SECONDARY},  {@link NON_UNIQUE} and {@link UNCLUSTERED}
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
    public Index(Column column, boolean ascending) throws SQLException
    {
        this("dbtune_" + IN_MEMORY_ID.get() + "_index",
                column, ascending, SECONDARY, NON_UNIQUE, UNCLUSTERED);
    }

    /**
     * Creates an index containing the given column. The name of the index is defaulted to {@code 
     * "dbtune_" + getId() + "_index"}. The index is assumed to be {@link SECONDARY},  {@link 
     * NON_UNIQUE} and {@link UNCLUSTERED}.
     *
     * @param name
     *     name of the index
     * @param columns
     *     columns that will define the index
     * @throws SQLException
     *     if column list empty; if schema already contains an index with the defaulted name; if not 
     *     all of the columns in the list correspond to the same table.
     */
    public Index(String name, List<Column> columns) throws SQLException
    {
        this(name, columns, (List<Boolean>) null);
    }

    /**
     * Creates an index containing the given column. The column is taken as being in ascending 
     * order. The index is assumed to be {@link SECONDARY},  {@link NON_UNIQUE} and {@link 
     * UNCLUSTERED}
     *
     * @param name
     *     name of the index
     * @param columns
     *     columns that will define the index
     * @param ascending
     *     indicates whether or not the corresponding column is sorted in ascending or ascending 
     *     order.
     * @throws SQLException
     *     if column list empty; if schema already contains an index with the defaulted name; if not 
     *     all of the columns in the list correspond to the same table.
     */
    public Index(String name, List<Column> columns, Map<Column, Boolean> ascending)
        throws SQLException
    {
        this(name, columns, ascending, SECONDARY, NON_UNIQUE, UNCLUSTERED);        
    }

    /**
     * Creates an index containing the given column. The name of the index is defaulted to {@code 
     * "dbtune_" + getId() + "_index"}. The index is assumed to be {@link SECONDARY},  {@link 
     * NON_UNIQUE} and {@link UNCLUSTERED}
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
    public Index(List<Column> columns, Map<Column, Boolean> ascending)
        throws SQLException
    {
        this("dbtune_" + IN_MEMORY_ID.get() + "_index", columns, ascending);
    }

    /**
     * Creates an index with the given name, column, primary, uniqueness and clustering values. The 
     * column is taken as being in ascending order.
     *
     * @param name
     *     name of the index
     * @param column
     *     column that will define the index
     * @param ascending
     *     indicates whether or not the corresponding column is sorted in ascending or ascending 
     *     order.
     * @param primary
     *     whether or not the index is primary
     * @param unique
     *     whether the index is unique or not.
     * @param clustered
     *     whether the corresponding table is clustered on this index
     * @throws SQLException
     *     if schema already contains an index with the given name.
     */
    public Index(
            String name,
            Column column,
            boolean ascending,
            boolean primary,
            boolean unique,
            boolean clustered)
        throws SQLException
    {
        this((Schema) column.getContainer().getContainer(), name, primary, unique, clustered);
        this.add(column, ascending);
    }

    /**
     * Creates an index from the given columns,  primary,  uniqueness and clustering values. Columns
     * are taken as ordered in ascending order.
     *
     * @param name
     *     name of the index
     * @param columns
     *     columns that will define the index
     * @param primary
     *     whether or not the index is primary
     * @param unique
     *     whether or not the index is unique
     * @param clustered
     *     whether the corresponding table is clustered on this index
     * @throws SQLException
     *     if column list empty; if schema already contains an index with the given name; if not all 
     *     of the columns in the list correspond to the same table.
     */
    public Index(
            String name, List<Column> columns, boolean primary, boolean unique, boolean clustered)
        throws SQLException
    {
        this(null, name, columns, null, primary, unique, clustered);
    }

    /**
     * Creates an index from the given columns,  primary,  uniqueness and clustering values.
     *
     * @param name
     *     name of the index
     * @param columns
     *     columns that will define the index
     * @param ascending
     *     indicates whether or not the corresponding column is sorted in ascending or ascending 
     *     order.
     * @param primary
     *     whether or not the index is primary
     * @param unique
     *     whether or not the index is unique
     * @param clustered
     *     whether the corresponding table is clustered on this index
     * @throws SQLException
     *     if column list empty; if schema already contains an index with the given name; if not all 
     *     of the columns in the list correspond to the same table.
     */
    public Index(
            String name,
            List<Column> columns,
            Map<Column, Boolean> ascending,
            boolean primary,
            boolean unique,
            boolean clustered)
        throws SQLException
    {
        this(null, name, columns, null, primary, unique, clustered);

        this.ascendingColumn.clear();

        for (Column c : columns) {
            if (ascending.get(c) == null)
                throw new RuntimeException("must have value for column " + c);
            this.ascendingColumn.add(ascending.get(c));
        }
    }
    
    /**
     * Creates an index from the given columns,  primary,  uniqueness and clustering values. The 
     * index is assumed to be {@link SECONDARY},  {@link NON_UNIQUE} and {@link UNCLUSTERED}.
     * 
     * @param columns
     *     columns that will define the index
     * @param ascending
     *     indicates whether or not the corresponding column is sorted in ascending or ascending 
     *     order.
     * @param primary
     *     whether or not the index is primary
     * @param unique
     *     whether or not the index is unique
     * @param clustered
     *     whether the corresponding table is clustered on this index
     * @throws SQLException
     *     if column list empty; if schema already contains an index with the given name; if not all 
     *     of the columns in the list correspond to the same table.
     */
    public Index(
            List<Column> columns,
            List<Boolean> ascending,
            boolean primary,
            boolean unique,
            boolean clustered) throws SQLException
    {
        this("dbtune_" + IN_MEMORY_ID.get() + "_index", columns, ascending, primary, unique, 
                clustered);
    }
   

    /**
     * Creates an index from the given columns,  primary,  uniqueness and clustering values. The 
     * index is assumed to be {@link SECONDARY},  {@link NON_UNIQUE} and {@link UNCLUSTERED}.
     *
     * @param name
     *     name of the index
     * @param columns
     *     columns that will define the index
     * @param ascending
     *     indicates whether or not the corresponding column is sorted in ascending or ascending 
     *     order.
     * @throws SQLException
     *     if column list empty; if schema already contains an index with the given name; if not all 
     *     of the columns in the list correspond to the same table.
     */
    public Index(String name, List<Column> columns, List<Boolean> ascending)throws SQLException
    {
        this(name, columns, ascending, SECONDARY, NON_UNIQUE, UNCLUSTERED);
    }

    /**
     * Creates an index from the given columns,  primary,  uniqueness and clustering values.
     *
     * @param name
     *     name of the index
     * @param columns
     *     columns that will define the index
     * @param ascending
     *     indicates whether or not the corresponding column is sorted in ascending or ascending 
     *     order.
     * @param primary
     *     whether or not the index is primary
     * @param unique
     *     whether or not the index is unique
     * @param clustered
     *     whether the corresponding table is clustered on this index
     * @throws SQLException
     *     if column list empty; if schema already contains an index with the given name; if not all 
     *     of the columns in the list correspond to the same table.
     */
    public Index(
            String name, 
            List<Column> columns, 
            List<Boolean> ascending, 
            boolean primary, 
            boolean unique, 
            boolean clustered)
        throws SQLException
    {
        this(null, name, columns, ascending, primary, unique, clustered);
    }

    /**
     * Creates an empty index. The index is assumed to be {@link SECONDARY},  {@link NON_UNIQUE} and 
     * {@link UNCLUSTERED}
     *
     * @param schema
     *     schema where the index will be contained.
     * @param name
     *     name of the index
     * @throws SQLException
     *     if schema already contains an index with the defaulted name
     */
    public Index(Schema schema, String name) throws SQLException
    {
        this(schema,  name,  new ArrayList<Column>(), 
                new ArrayList<Boolean>(),  SECONDARY,  NON_UNIQUE,  UNCLUSTERED);
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
        this(schema, name, new ArrayList<Column>(), new ArrayList<Boolean>(), primary, unique, 
                clustered);
    }

    /**
     * Creates an index from the given columns,  primary,  uniqueness and clustering values.
     *
     * @param schema
     *     schema where the index will be contained.
     * @param name
     *     name of the index
     * @param columns
     *     columns that will define the index
     * @param ascending
     *     indicates whether or not the corresponding column is sorted in ascending or ascending 
     *     order.
     * @param unique
     *     whether or not the index is unique
     * @param primary
     *     whether or not the index is primary
     * @param clustered
     *     whether the corresponding table is clustered on this index
     * @throws SQLException
     *     if column list empty; if schema already contains an index with the given name; if not all 
     *     of the columns in the list correspond to the same table.
     */
    private Index(
            Schema schema, 
            String name, 
            List<Column> columns, 
            List<Boolean> ascending, 
            boolean primary, 
            boolean unique, 
            boolean clustered)
        throws SQLException
    {
        super(name);

        Schema sch = schema;
        List<Boolean> localCopyAscending  = new ArrayList<Boolean>();

        if (sch == null && columns.size() == 0)
            throw new SQLException("Column list should have at least one element");
        
        Table table = null;

        if (sch == null) {
            table     = columns.get(0).getTable();
            sch       = table.getSchema();
            container = schema;
        }
        
        this.ascendingColumn = new ArrayList<Boolean>();

        if (ascending == null) {
            for (int i = 0; i < columns.size(); i++)
                localCopyAscending.add(ASCENDING);
        } else if (ascending.size() != columns.size()) {
            throw new SQLException("Incorrect number of ascending/descending values");
        } else {
            localCopyAscending = new ArrayList<Boolean>(ascending);
        }
        
        for (int i = 0; i < columns.size(); i++) {
            if (table == null)
                table = (Table) columns.get(i).container;

            if (table != columns.get(i).container)
                throw new SQLException("Columns from different tables");
            
            add(columns.get(i), localCopyAscending.get(i));
        }

        this.type       = UNKNOWN;
        this.primary    = primary;
        this.unique     = unique;
        this.clustered  = clustered;
        this.scanOption = NON_REVERSIBLE;
        this.container  = sch;
        this.inMemoryID = Index.IN_MEMORY_ID.getAndIncrement();

        container.add(this);
    }

    /**
     * Copy constructor.
     *
     * @param other
     *     other index being copied into the new one
     */
    public Index(Index other)
    {
        super(other);

        this.inMemoryID   = other.inMemoryID;
        this.type         = other.type;
        this.unique       = other.unique;
        this.primary      = other.primary;
        this.clustered    = other.clustered;
        this.materialized = other.materialized;
        this.ascendingColumn   = other.ascendingColumn;
        this.scanOption   = other.scanOption;
    }

    /**
     * Returns the list ascending values for each column contained in the index.
     *
     * @return
     *      list of ascending values
     */
    public List<Boolean> getAscending()
    {
        return new ArrayList<Boolean>(ascendingColumn);
    }

    /**
     * Sets the type of index. Either UNKNOWN, B_TREE or BITMAP.
     *
     * @param type
     *     one of the available fields.
     */
    public void setType(int type)
    {
        switch(type) {
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
     * {@inheritDoc}
     */
    @Override
    public int getId()
    {
        return this.inMemoryID;
    }

    /**
     * Whether or not this index is reversible. An index is reversible if it can be scanned in 
     * reverse order. For example, assume two indexes on one column {@code A} of a table are created 
     * with {@code ASC} and {@code DESC} constraints, respectively. A reversible index can replace 
     * them since scanning it in reverse order is equivalent to having an index defined with the 
     * opposite constraint.
     *
     * @return
     *     <code>true</code> if reversible; <code>false</code> otherwise
     */
    public boolean isReversible()
    {
        if (scanOption == REVERSIBLE)
            return true;

        return false;
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
     * materialized,  only one of them can return <code>true</code>.
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
     * Whether or not this index is materialized,  i.e. exists in the database and not only as an 
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
     * #getContainer} does but without requiring the user to cast. In other words,  the following is 
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
     * does nothing,  i.e. repetitions aren't allowed
     *
     * @param column
     *     new column to be inserted to the sequence
     * @param ascending
     *     {@code true} if the column is in ascending order. {@code false} if ascending.
     * @throws SQLException
     *     if column is already contained in the index
     */
    public void add(Column column,  Boolean ascending) throws SQLException
    {
        if (containees.size() != 0 && containees.get(0).getContainer() != column.container)
            throw new SQLException("Table " + column.getContainer().getName() +
                                   " doesn't correspond to " + containees.get(0).getContainer());

        super.add(column);
        this.ascendingColumn.add(ascending);
    }

    /**
     * adds a new column to the index in ascending order. If the column is already contained it does 
     * nothing,  i.e.  repetitions aren't allowed
     *
     * @param column
     *     new column to be inserted to the sequence
     * @throws SQLException
     *     if column is already contained in the index
     */
    public void add(Column column) throws SQLException
    {
        add(column, ASCENDING);
    }

    /**
     * Returns the ascending value for the given column.
     *
     * @param column
     *     the column for which the ascending value is being requested.
     * @return
     *     <code>true</code> the column is ascending; <code>false</code> if ascending
     */
    public boolean isAscending(Column column)
    {
        try {
            return ascendingColumn.get(containees.indexOf(column));
        } catch (IndexOutOfBoundsException ex) {
            throw new RuntimeException(new SQLException(ex));
        }
    }

    /**
     * Whether the given index covers this one without taking into account the order. An index a is 
     * covered by another b if a's columns are contained in b's and they're in the same {@link 
     * #isAscending ascending} order.
     *
     * @param other
     *     index that may (or not) cover this one.
     * @return
     *     {@code true} if this index is covered by the given one; {@code false} otherwise
     */
    public boolean isCoveredByIgnoreOrder(Index other)
    {
        if (size() == 0 || other.size() < this.size() || !other.columns().containsAll(columns()))
            return false;

        for (Column col : columns()) {
            if (isAscending(col) && !other.isAscending(col))
                return false;
        }

        return true;
    }

    /**
     * Whether the given index covers this one. An index a is covered by another b if a's columns 
     * are a prefix of b's and with the same {@link #isAscending ascending} value.
     *
     * @param other
     *     index that may (or not) cover this one.
     * @return
     *     {@code true} if this index is covered by the given one; {@code false} otherwise
     */
    public boolean isCoveredBy(Index other)
    {
        if (size() == 0 || other.size() < this.size())
            return false;

        for (int i = 0; i < size(); i++) {
            if (containees.get(i) != other.containees.get(i))
                return false;

            if (ascendingColumn.get(i) != other.ascendingColumn.get(i))
                return false;
        }

        return true;
    }

    /**
     * Set the identifier of the object.
     * 
     * @param id
     *      the given ID
     */
    public void setId(int id)
    {
        this.inMemoryID = id;
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

        Index o = (Index) other;

        if (this.size() != o.size())
            return false;

        for (int i = 0; i < size(); i++) {
            if (containees.get(i) != o.containees.get(i))
                return false;

            if (ascendingColumn.get(i) != o.ascendingColumn.get(i))
                return false;
        }

        if (type != o.type ||
                unique != o.unique ||
                primary != o.primary ||
                clustered != o.clustered ||
                materialized != o.materialized ||
                scanOption != o.scanOption)
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
    public List<Column> columns()
    {
        return new ArrayList<Column>(Objects.<List<Column>>as(containees));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isValid(DatabaseObject dbo)
    {
        return dbo instanceof Column;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public String toString()
    {
        String str = "[";

        for (Column col : this)
            str += "+" + col + (isAscending(col) ? "(A)" : "(D)");

        str += "]";

        return str;
    }
}
