package edu.ucsc.dbtune.workload;

/**
 * Represents a SQL statement. Each {@code SQLStatement} object is tied to a {@code String} object 
 * that contains the actual literal contents of the SQL statement.
 *
 * @author Ivo Jimenez
 */
public class SQLStatement
{
    /** category of statement. */
    private SQLCategory category;

    /** literal contents of the statement. */
    private String sql;

    /**
     * Constructs a {@code SQLStatement}. The constructor tries to infer the category of the 
     * statement using the {@link SQLCategory#from} method.
     *
     * @param sql
     *      a sql statement.
     * @see SQLCategory#from
     */
    public SQLStatement(String sql)
    {
        this(sql, SQLCategory.from(sql));
    }

    /**
     * Constructs a {@code SQLStatement} given its category and the literal contents.
     *
     * @param category
     *      the corresponding {@link SQLCategory} representing the category of statement.
     * @param sql
     *      a sql statement.
     */
    public SQLStatement(String sql, SQLCategory category)
    {
        this.category = category;
        this.sql      = sql;
    }

    /**
     * Returns the category of statement.
     *
     * @return
     *     a sql category.
     * @see SQLCategory
     */
    public SQLCategory getSQLCategory()
    {
        return category;
    }

    /**
     * Returns the actual SQL statement.
     *
     * @return
     *     a string containing the SQL statement that was optimized.
     */
    public String getSQL()
    {
        return sql;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int hashCode()
    {
        return sql.hashCode();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean equals(Object obj)
    {
        if (this == obj)
            return true;
    
        if (!(obj instanceof SQLStatement))
            return false;
    
        SQLStatement o = (SQLStatement) obj;

        if (category.isSame(o.category) && sql.equals(o.sql))
            return true;

        return false;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString()
    {
        return "[ category=" + category +
               " text=\"" + sql + "\"]";
    }
}
