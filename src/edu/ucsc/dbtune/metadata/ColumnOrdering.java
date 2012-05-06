package edu.ucsc.dbtune.metadata;

import java.sql.SQLException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

/**
 * Represents the ordering of a set of columns interesting order. All the columns contained in an 
 * ordering should refer to the sames table.
 *
 * @author Ivo Jimenez
 */
public class ColumnOrdering
{
    /** ascending. */
    public static final int ASCENDING = 0;
    /** ascending. */
    public static final int ASC = ASCENDING;
    /** descending. */
    public static final int DESCENDING = 1;
    /** descending. */
    public static final int DESC = DESCENDING;
    /** reversible. */
    public static final int REVERSIBLE = 2;
    /** any ordering is valid. */
    public static final int ANY = 3;
    /** unknown ordering. */
    public static final int UNKNOWN = 4;

    protected List<Column> columns;
    protected Map<Column, Integer> ordering;

    /**
     * Creates an ordering with default (@link #ASCENDING) value.
     *
     * @param columns
     *      list of columns in the ordering. The ordering of the list determines the ordering
     * @throws SQLException
     *      if not all columns correspond to the same table
     */
    public ColumnOrdering(List<Column> columns) throws SQLException
    {
        HashMap<Column, Integer> orderingMap = new HashMap<Column, Integer>();

        for (Column c : columns)
            orderingMap.put(c, ASC);

        init(columns, orderingMap);
    }

    /**
     * Creates an ordering out of an index.
     *
     * @param index
     *      an index
     * @throws SQLException
     *      if not all columns correspond to the same table
     */
    public ColumnOrdering(Index index) throws SQLException
    {
        Map<Column, Integer> orderings = new HashMap<Column, Integer>();

        for (Column c : index.columns())
            if (index.isAscending(c))
                orderings.put(c, ASC);
            else
                orderings.put(c, DESC);
        
        init(index.columns(), orderings);
    }

    /**
     * Creates a column ordering with given column and order.
     *
     * @param column
     *     column that will define the index
     * @param ordering
     *     indicates the type of ordering of the column
     * @throws SQLException
     *      if the schema of the table is null or can't be retrieved
     */
    public ColumnOrdering(Column column, int ordering) throws SQLException
    {
        List<Column> columnList = new ArrayList<Column>();
        HashMap<Column, Integer> orderingMap = new HashMap<Column, Integer>();

        columnList.add(column);
        orderingMap.put(column, ordering);

        init(columnList, orderingMap);
    }
    
    /**
     * Creates an interesting order instance.
     *
     * @param columns
     *      column that will define the index
     * @param ordering
     *      the ordering values for the columns
     * @throws SQLException
     *      if the schema of the table is null or can't be retrieved
     */
    public ColumnOrdering(List<Column> columns, Map<Column, Integer> ordering)
        throws SQLException
    {
        init(columns, ordering);
    }

    /**
     * Creates an interesting order instance.
     *
     * @param columns
     *      column that will define the index
     * @param ordering
     *      the ordering values for the columns
     * @throws SQLException
     *      if the schema of the table is null or can't be retrieved
     */
    public ColumnOrdering(List<Column> columns, List<Boolean> ordering) throws SQLException
    {
        if (columns.size() != ordering.size())
            throw new SQLException(
                    "Number of ordering entries in map should be equal to number of columns");

        HashMap<Column, Integer> orderingMap = new HashMap<Column, Integer>();

        for (int i = 0; i < columns.size(); i++)
            if (ordering.get(i))
                orderingMap.put(columns.get(i), ASC);
            else
                orderingMap.put(columns.get(i), DESC);

        init(columns, orderingMap);
    }
    
    /**
     * Copy constructor.
     *
     * @param other
     *      other object being copied
     * @throws SQLException
     *      if the schema of the table is null or can't be retrieved
     */
    public ColumnOrdering(ColumnOrdering other)
        throws SQLException
    {
        init(other.columns, other.ordering);
    }

    /**
     * initializes the ordering with the given columns and sorting values.
     *
     * @param columns
     *      columns in the ordering
     * @param ordering
     *      map each column in {@code columns} to an ordering value
     * @throws SQLException
     *      if {@code columns} is empty; not all columns in the list correspond to the same table; 
     *      size of map is distinct to size of list; a column in the list is missing an ordering 
     *      value in the map (i.e. {@code ordering.get(column) == null} is {@code true}).
     */
    private void init(List<Column> columns, Map<Column, Integer> ordering) throws SQLException
    {
        if (columns.isEmpty())
            throw new SQLException("Columns can't be empty");

        if (columns.size() != ordering.size())
            throw new SQLException(
                    "Number of ordering entries in map should be equal to number of columns");

        this.columns = new ArrayList<Column>(columns);
        this.ordering = new HashMap<Column, Integer>(ordering);

        Table table = columns.get(0).getTable();

        for (Column c : this.columns)
            if (table != c.getTable())
                throw new SQLException("Columns from different tables");

        for (Column c : this.columns)
            if (this.ordering.get(c) == null)
                throw new SQLException("No ordering value for column " + c + " in given map");
            else if (!isValid(this.ordering.get(c)))
                throw new SQLException(this.ordering.get(c) + " value for " + c + " invalid");
    }

    /**
     * Returns the table that the columns correspond to.
     *
     * @return
     *      table
     */
    public Table getTable()
    {
        if (getColumns().size() == 0)
            throw new RuntimeException("Empty ordering");

        return getColumns().get(0).getTable();
    }

    /**
     * Returns the size, determined by the number of columns in the ordering.
     *
     * @return
     *      size of the ordering
     */
    public int size()
    {
        return this.columns.size();
    }

    /**
     * the list of columns of the ordering.
     *
     * @return
     *      the table
     */
    public List<Column> getColumns()
    {
        return new ArrayList<Column>(this.columns);
    }

    /**
     * the ordering value for the given column.
     *
     * @param column
     *      the column being checked
     * @return
     *      the ordering value
     * @throws NoSuchElementException
     *      if the given column isn't part of the ordering
     */
    public int getOrdering(Column column)
    {
        if (!columns.contains(column) || ordering.get(column) == null)
            throw new NoSuchElementException("Column " + column + " not part of ordering");

        return this.ordering.get(column);
    }

    /**
     * the ordering values for each column.
     *
     * @return
     *      the ordering values
     */
    public Map<Column, Integer> getOrderings()
    {
        return new HashMap<Column, Integer>(this.ordering);
    }

    /**
     * Whether the column has the given ordering.
     *
     * @param column
     *      the column being checked
     * @param value
     *      the value being checked
     * @return
     *      {@code true} if the value of {@code column} is {@code value}; {@code false} otherwise.
     * @throws NoSuchElementException
     *      if the given column isn't part of the ordering
     */
    public boolean is(Column column, int value)
    {
        if (!columns.contains(column) || ordering.get(column) == null)
            throw new NoSuchElementException("Column " + column + " not part of ordering");

        return ordering.get(column) == value;
    }

    /**
     * Whether an ordering value is valid.
     *
     * @param orderingValue
     *      the value being checked
     * @return
     *      {@code true} if the value is {@link #ASCENDING}, {@link #DESCENDING} or {@link #ANY}; 
     *      {@code false} otherwise.
     */
    public static boolean isValid(int orderingValue)
    {
        if (orderingValue == ASC || orderingValue == DESC || orderingValue == ANY)
            return true;

        return false;
    }

    /**
     * @param index
     *      index
     * @return
     *      {@code true} if order of {@code index} is equivalent; {@code false} otherwise
     */
    public boolean isSameOrdering(Index index)
    {
        if (index.size() != columns.size())
            return false;

        if (!columns.equals(index.columns()))
            return false;

        for (Column c : index.columns())
            if (index.isAscending(c) && getOrdering(c) != ASC)
                return false;
            else if (!index.isAscending(c) && getOrdering(c) != DESC)
                return false;

        return true;
    }

    /**
     * Whether the given index covers this one. An index a is covered by another b if a's columns 
     * are a prefix of b's and with the same {@link #isAscending ascending} value.
     *
     * @param index
     *     index that may (or not) cover this one.
     * @return
     *     {@code true} if this index is covered by the given one; {@code false} otherwise
     */
    public boolean isCoveredBy(Index index)
    {
        ColumnOrdering co;

        try {
            co = new ColumnOrdering(index);
        } catch (SQLException ex) {
            throw new RuntimeException(ex);
        }

        return isCoveredBy(co);
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
    public boolean isCoveredBy(ColumnOrdering other)
    {
        if (other.columns.size() < columns.size())
            return false;

        for (int i = 0; i < columns.size(); i++)
            if (columns.get(i) != other.columns.get(i) ||
                    !covers(ordering.get(columns.get(i)), other.ordering.get(columns.get(i))))
                return false;

        return true;
    }

    /**
     * Whether the given index covers this one without taking into account the order. An index a is 
     * covered by another b if a's columns are contained in b's and they're in the same {@link 
     * #isAscending ascending} order.
     *
     * @param index
     *     index that may (or not) cover this one.
     * @return
     *     {@code true} if this index is covered by the given one; {@code false} otherwise
     */
    public boolean isCoveredByIgnoreOrder(Index index)
    {
        ColumnOrdering co;

        try {
            co = new ColumnOrdering(index);
        } catch (SQLException ex) {
            throw new RuntimeException(ex);
        }

        return isCoveredByIgnoreOrder(co);
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
    public boolean isCoveredByIgnoreOrder(ColumnOrdering other)
    {
        if (other.columns.size() < columns.size() || !other.columns.containsAll(columns))
            return false;

        for (Column col : columns)
            if (!covers(ordering.get(col), other.ordering.get(col)))
                return false;

        return true;
    }

    /**
     * @param o1
     *      first ordering
     * @param o2
     *      second ordering
     * @return
     *      {@code true} if order {@code o1} covers {@code o2}; {@code false} otherwise
     */
    public static boolean covers(int o1, int o2)
    {
        if (o1 == o2 || o1 == ANY || o2 == ANY)
            return true;

        return false;
    }

    /**
     * @param o1
     *      first ordering
     * @param o2
     *      second ordering
     * @return
     *      {@code true} if order {@code o1} covers {@code o2}; {@code false} otherwise
     */
    public static boolean covers(int o1, boolean o2)
    {
        if (o1 == ASC && o2)
            return true;

        if ((o1 == DESC || o1 == ANY) && !o2)
            return true;

        return false;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int hashCode()
    {
        return columns.hashCode() + ordering.hashCode();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean equals(Object other)
    {
        if (!(other instanceof ColumnOrdering))
            return false;

        ColumnOrdering o = (ColumnOrdering) other;

        if (columns.equals(o.columns) && ordering.equals(o.ordering))
            return true;

        return false;
    }
}
