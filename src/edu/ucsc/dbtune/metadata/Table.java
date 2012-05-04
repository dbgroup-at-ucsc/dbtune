package edu.ucsc.dbtune.metadata;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import edu.ucsc.dbtune.util.Objects;

/**
 * Metadata for a table.
 *
 * @author Ivo Jimenez
 */
public class Table extends DatabaseObject implements Iterable<Column>
{
    /**
     * Default value
     */
    private static final long serialVersionUID = 1L;

    protected int type;

    public static final int REGULAR = 1;
    public static final int MDC     = 2;

    /**
     * Constructor
     *
     * @param name
     *     name of the table
     * @param schema
     *     schema where the new table will be contained
     * @throws SQLException
     *     if a table with the given name is already contained in the schema
     */
    public Table(Schema schema, String name) throws SQLException
    {
        super(schema, name);

        this.type = REGULAR;
    }

    /**
     * Copy Constructor
     *
     * @param other
     *     object being copied
     */
    protected Table(Table other)
    {
        super(other);

        type = other.type;
    }

    /**
     * Returns the schema that contains this table. Convenience method that accomplishes what {@link 
     * #getContainer} does but without requiring the user to cast. In other words, the following is 
     * true {@code getSchema() == (Schema)getContainer()}.
     *
     * @return
     *     the schema that contains this object
     */
    public Schema getSchema()
    {
        return (Schema) container;
    }

    /**
     * Finds a column whose name matches the given argument. Convenience method that accomplishes 
     * what {@link #find} does but without making requiring the user to cast. In other words, the 
     * following is true {@code findColumn("name") == (Column)find("name")}.
     *
     * @param name
     *     name of the object that is searched for in <code>this</code> table.
     * @return
     *     the column that has the given name; null if not found
     */
    public Column findColumn(String name)
    {
        return (Column) find(name);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public DatabaseObject newContainee(String name) throws SQLException
    {
        return new Column(this,name);
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
    public boolean equalsContent(Object other)
    {
        throw new RuntimeException("not implemented yet");
    }
}
