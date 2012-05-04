package edu.ucsc.dbtune.metadata;

import java.sql.SQLException;

import static edu.ucsc.dbtune.metadata.SQLTypes.isValidType;

/**
 * POJO for representing column metadata
 *
 * @author Ivo Jimenez
 */
public class Column extends DatabaseObject 
{
    /**
     * Default value
     */
    private static final long serialVersionUID = 1L;
    
    
    int     type;
    boolean isNull;
    boolean isDefault;
    String  defaultValue;

    /**
     * Creates a column with the given name.
     *
     * @param table
     *     table where the new column will be contained
     * @param name
     *     name assigned to the column.
     * @throws SQLException
     *     if a column with the given name is already contained in the table
     */
    public Column(Table table, String name) throws SQLException
    {
        this(table,name,SQLTypes.UNKNOWN);
    }

    /**
     * Creates a column with the given name and type. The type should be one of the values defined 
     * in SQLTypes.
     *
     * @param table
     *     table where the new column will be contained
     * @param name
     *     name assigned to the column.
     * @param type
     *     data type
     * @throws SQLException
     *     if a column with the given name is already contained in the table
     */
    public Column(Table table, String name, int type) throws SQLException
    {
        super(table, name);

        this.isNull       = true;
        this.isDefault    = true;
        this.defaultValue = "";
        
        setDataType(type);
    }

    /**
     * copy constructor
     *
     * @param other
     *     column object being copied
     */
    protected Column(Column other) throws SQLException
    {
        super(other);

        this.type         = other.type;
        this.isNull       = other.isNull;
        this.isDefault    = other.isDefault;
        this.defaultValue = other.defaultValue;
    }

    /**
     * Assigns the type of the column. The type should be one of the values defined in SQLTypes.
     *
     * @param type
     *     type of the column
     */
    protected void setDataType(int type)
    {
        if (!isValidType(type))
            throw new RuntimeException("Invalid data type " + type);

        this.type = type;
    }

    /**
     * The data type of the column
     *
     * @return
     *     one of the values from SQLTypes
     */
    public int getDataType()
    {
        return type;
    }

    /**
     * Returns the size of the column in bytes.
     *
     * @return
     *     size of the columns in bytes
     */
    @Override
    public long getBytes()
    {
        if (type == SQLTypes.VARCHAR ||
            type == SQLTypes.CHARACTER ||
            type == SQLTypes.NCHAR ||
            type == SQLTypes.NUMERIC ||
            type == SQLTypes.DECIMAL ||
            type == SQLTypes.FLOAT)
        {
            return bytes;
        }

        return SQLTypes.getSize(type);
    }

    /**
     * Returns the table that contains this column. Convenience method that accomplishes what {@link 
     * #getContainer} does but without requiring the user to cast. In other words, the following is 
     * true {@code getTable() == (Table)getContainer()}.
     *
     * @return
     *     the schema that contains this object
     */
    public Table getTable()
    {
        return (Table) container;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public DatabaseObject newContainee(String name) throws SQLException
    {
        throw new SQLException("Column can't contain objects");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isValid(DatabaseObject dbo)
    {
        return false;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean equalsContent(Object other)
    {
        throw new RuntimeException("not implemented yet");
    }
}
